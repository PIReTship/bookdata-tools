stage ClusterStats {
    cmd "jupytext --to ipynb --execute ClusterStats.py"
    dep "ClusterStats.py"
    dep "book-links/cluster-stats.parquet"
    out -nocache "ClusterStats.ipynb"
}

stage LinkageStats {
    cmd "jupytext --to ipynb --execute LinkageStats.py"
    dep "LinkageStats.py"
    dep "book-links/gender-stats.csv"
    out -nocache "LinkageStats.ipynb"
    metric "book-coverage.json"
}

  html-report:
    foreach:
    - LinkageStats
    - ClusterStats
    do:
      cmd: jupyter nbconvert --to html "${item}.ipynb"
      deps:
      - ${item}.ipynb
      outs:
      - ${item}.html
