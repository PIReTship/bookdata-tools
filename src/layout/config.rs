use std::fs::read_to_string;

use anyhow::Result;
use log::*;
use serde::Deserialize;

use super::path::BDPath;

const CFG_PATH: BDPath<'static> = BDPath::new("config.yaml");

#[derive(Debug, Deserialize, Clone)]
pub struct DSConfig {
    pub enabled: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GRConfig {
    pub enabled: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub bx: DSConfig,
    pub az2014: DSConfig,
    pub az2018: DSConfig,
    pub goodreads: GRConfig,
}

impl Config {
    pub fn ds_enabled(&self, name: &str) -> bool {
        let (name, _qual) = if let Some((n, q)) = name.split_once("-") {
            (n, Some(q))
        } else {
            (name, None)
        };
        match name {
            "loc" | "LOC" => true,
            "openlib" | "openlibrary" | "OL" => true,
            "goodreads" | "GR" => self.goodreads.enabled,
            "az2014" | "AZ14" => self.az2014.enabled,
            "az2018" | "AZ18" => self.az2018.enabled,
            "bx" | "BX" => self.bx.enabled,
            _ => panic!("unsupported data set {}", name),
        }
    }
}

/// Load the configuration for this project.
pub fn load_config() -> Result<Config> {
    let path = CFG_PATH.resolve()?;
    debug!("reading configuration {}", path.display());
    let f = read_to_string(&path)?;
    let cfg: Config = serde_yaml::from_str(&f)?;
    Ok(cfg)
}
