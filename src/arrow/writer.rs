use std::borrow::Cow;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use log::*;
use polars::io::parquet::BatchedWriter;
use polars::prelude::{ArrowSchema, DataFrame, ParquetCompression, ParquetWriter, ZstdLevel};
use polars_arrow::array::Array as PArray;
use polars_arrow::chunk::Chunk as PChunk;
use polars_parquet::write::{
    transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version, WriteOptions,
};

use super::row::{vec_to_df, TableRow};
use crate::io::object::{ObjectWriter, ThreadObjectWriter, UnchunkWriter};
use crate::io::DataSink;

const BATCH_SIZE: usize = 32 * 1024 * 1024;
const ZSTD_LEVEL: i32 = 3;

/// Open a Parquet writer using BookData defaults.
pub fn open_parquet_writer<P: AsRef<Path>>(
    path: P,
    schema: ArrowSchema,
) -> Result<FileWriter<File>> {
    let compression = CompressionOptions::Zstd(None);
    let options = WriteOptions {
        write_statistics: true,
        version: Version::V2,
        compression,
        data_pagesize_limit: None,
    };

    info!("creating Parquet file {:?}", path.as_ref());
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    let writer = FileWriter::try_new(file, schema, options)?;

    Ok(writer)
}

/// Open a Polars data frame Parquet writer using BookData defaults.
pub fn open_polars_writer<P: AsRef<Path>>(path: P) -> Result<ParquetWriter<File>> {
    info!("creating Parquet file {:?}", path.as_ref());
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    let writer = ParquetWriter::new(file).with_compression(ParquetCompression::Zstd(Some(
        ZstdLevel::try_new(ZSTD_LEVEL)?,
    )));

    Ok(writer)
}

/// Save a data frame to a Parquet file.
pub fn save_df_parquet<P: AsRef<Path>>(df: DataFrame, path: P) -> Result<()> {
    let path = path.as_ref();
    debug!("writing file {}", path.display());
    debug!("{}: schema {:?}", path.display(), df.schema());
    let mut df = df;
    let writer = open_polars_writer(path)?;
    let size = writer
        .with_row_group_size(Some(BATCH_SIZE))
        .finish(&mut df)?;
    debug!("{}: wrote {}", path.display(), friendly::bytes(size));
    Ok(())
}

/// Parquet table writer.
///
/// A table writer is an [ObjectWriter] for structs implementing [TableRow], that writes
/// them out to a Parquet file.
pub struct TableWriter<R: TableRow + Send + Sync + 'static> {
    _phantom: PhantomData<R>,
    writer: Option<UnchunkWriter<R, ThreadObjectWriter<'static, Vec<R>>>>,
    out_path: Option<PathBuf>,
    row_count: usize,
}

impl<R> TableWriter<R>
where
    R: TableRow + Send + Sync + 'static,
{
    /// Open a table writer for a path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();

        // extract struct schema
        let schema = R::schema();

        let writer = open_polars_writer(path)?;
        let writer = writer.batched(&schema)?;
        let writer = writer.with_transform(vec_to_df);
        let writer = ThreadObjectWriter::wrap(writer)
            .with_name(format!("write:{}", path.display()))
            .with_capacity(4)
            .spawn();
        let writer = UnchunkWriter::with_size(writer, BATCH_SIZE);
        let out_path = Some(path.to_path_buf());
        Ok(TableWriter {
            _phantom: PhantomData,
            writer: Some(writer),
            out_path,
            row_count: 0,
        })
    }
}

impl<R> TableWriter<R>
where
    R: TableRow + Send + Sync + 'static,
{
    fn display_path(&self) -> Cow<'static, str> {
        if let Some(p) = &self.out_path {
            format!("{}", p.display()).into()
        } else {
            "<unknown>".into()
        }
    }
}

impl<R> DataSink for TableWriter<R>
where
    R: TableRow + Send + Sync + 'static,
{
    fn output_files(&self) -> Vec<PathBuf> {
        match &self.out_path {
            None => Vec::new(),
            Some(p) => vec![p.clone()],
        }
    }
}

impl<R> ObjectWriter<R> for TableWriter<R>
where
    R: TableRow + Send + Sync + 'static,
{
    fn write_object(&mut self, row: R) -> Result<()> {
        let w = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow!("writer is closed"))?;

        w.write_object(row)?;
        self.row_count += 1;
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        if let Some(writer) = self.writer.take() {
            info!("closing Parquet writer for {}", self.display_path());
            writer.finish()?;
        } else {
            warn!("{}: writer already closed", self.display_path());
        }
        Ok(self.row_count)
    }
}

impl<R> Drop for TableWriter<R>
where
    R: TableRow + Send + Sync + 'static,
{
    fn drop(&mut self) {
        // make sure we closed the writer
        if self.writer.is_some() {
            error!("{}: Parquet table writer not closed", self.display_path());
        }
    }
}

/// Implementation of object writer for Polars Arrow writers
impl<W> ObjectWriter<PChunk<Box<dyn PArray + 'static>>> for FileWriter<W>
where
    W: Write,
{
    fn write_object(&mut self, chunk: PChunk<Box<dyn PArray + 'static>>) -> Result<()> {
        let schema = self.schema();
        let encodings: Vec<_> = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
            .collect();
        let options = self.options();
        let chunks = vec![Ok(chunk)];
        let groups = RowGroupIterator::try_new(chunks.into_iter(), &schema, options, encodings)?;
        for group in groups {
            self.write(group?)?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.end(None)?;
        Ok(0)
    }
}

/// Implementation of object writer for Polars Parquet batched writer
impl<W> ObjectWriter<DataFrame> for BatchedWriter<W>
where
    W: Write,
{
    fn write_object(&mut self, df: DataFrame) -> Result<()> {
        self.write_batch(&df)?;
        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        let size = BatchedWriter::finish(&mut self)?;
        Ok(size as usize)
    }
}
