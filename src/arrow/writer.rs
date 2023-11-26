use std::borrow::Cow;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::replace;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use arrow2::array::{Array, MutableArray, StructArray, TryExtend};
use arrow2::chunk::Chunk;
use arrow2::datatypes::*;
use arrow2::io::parquet::write::*;
use log::*;
use polars::io::parquet::BatchedWriter;
use polars::prelude::{
    ArrowSchema, DataFrame, ParquetCompression, ParquetWriter, ZstdLevel as PLZ,
};
use polars_arrow::array::Array as PArray;
use polars_arrow::chunk::Chunk as PChunk;
use polars_parquet::write as plw;

use super::row::{vec_to_df, FrameBuilder, TableRow};
use crate::io::object::{ObjectWriter, ThreadObjectWriter};
use crate::io::DataSink;

const BATCH_SIZE: usize = 32 * 1024 * 1024;

/// Open a Parquet writer using BookData defaults.
pub fn legacy_parquet_writer<P: AsRef<Path>>(path: P, schema: Schema) -> Result<FileWriter<File>> {
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

/// Open a Parquet writer using BookData defaults.
pub fn open_parquet_writer<P: AsRef<Path>>(
    path: P,
    schema: ArrowSchema,
) -> Result<plw::FileWriter<File>> {
    let compression = plw::CompressionOptions::Zstd(None);
    let options = plw::WriteOptions {
        write_statistics: true,
        version: plw::Version::V2,
        compression,
        data_pagesize_limit: None,
    };

    info!("creating Parquet file {:?}", path.as_ref());
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    let writer = plw::FileWriter::try_new(file, schema, options)?;

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
    let writer =
        ParquetWriter::new(file).with_compression(ParquetCompression::Zstd(Some(PLZ::try_new(9)?)));

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
    writer: Option<ThreadObjectWriter<Vec<R>>>,
    out_path: Option<PathBuf>,
    batch: Vec<R>,
    batch_size: usize,
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
        let writer = ThreadObjectWriter::with_capacity(writer, 32);
        let out_path = Some(path.to_path_buf());
        Ok(TableWriter {
            _phantom: PhantomData,
            writer: Some(writer),
            out_path,
            batch: Vec::with_capacity(BATCH_SIZE),
            batch_size: BATCH_SIZE,
            row_count: 0,
        })
    }

    fn write_batch(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let batch = replace(&mut self.batch, Vec::with_capacity(self.batch_size));
        if let Some(writer) = &mut self.writer {
            writer.write_object(batch)?;
        } else {
            error!("{}: writer closed", self.display_path());
            return Err(anyhow!("tried to write to closed writer"));
        }

        Ok(())
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
        self.batch.push(row);
        if self.batch.len() >= self.batch_size {
            self.write_batch()?;
        }
        self.row_count += 1;

        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        if !self.batch.is_empty() {
            self.write_batch()?;
        }
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

/// Implementation of object writer for Arrow2 writers
impl<W> ObjectWriter<Chunk<Box<dyn Array + 'static>>> for FileWriter<W>
where
    W: Write,
{
    fn write_object(&mut self, chunk: Chunk<Box<dyn Array + 'static>>) -> Result<()> {
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

/// Implementation of object writer for Polars Arrow writers
impl<W> ObjectWriter<PChunk<Box<dyn PArray + 'static>>> for plw::FileWriter<W>
where
    W: Write,
{
    fn write_object(&mut self, chunk: PChunk<Box<dyn PArray + 'static>>) -> Result<()> {
        let schema = self.schema();
        let encodings: Vec<_> = schema
            .fields
            .iter()
            .map(|f| plw::transverse(&f.data_type, |_| plw::Encoding::Plain))
            .collect();
        let options = self.options();
        let chunks = vec![Ok(chunk)];
        let groups =
            plw::RowGroupIterator::try_new(chunks.into_iter(), &schema, options, encodings)?;
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
