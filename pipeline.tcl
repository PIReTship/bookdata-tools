namespace import ::plumber::list_stages
namespace import ::plumber::stage_*

subdir loc-mds
subdir openlibrary
subdir viaf
subdir az2014
subdir az2018
subdir bx
# subdir goodreads

stage ClusterStats {
    cmd jupytext --to ipynb --execute ClusterStats.py
    dep ClusterStats.py
    dep book-links/cluster-stats.parquet
    out -nocache ClusterStats.ipynb
}

stage LinkageStats {
    cmd jupytext --to ipynb --execute LinkageStats.py
    dep LinkageStats.py
    dep book-links/gender-stats.csv
    out -nocache LinkageStats.ipynb
    out -metric book-coverage.json
}

stage html-report -items {
    LinkageStats
    ClusterStats
} {
    cmd jupyter nbconvert --to html {"${item}.ipynb"}
    dep {${item}.ipynb}
    out {${item}.html}
}

set pqlf [open parquets.log w]
set parquets [list]
foreach stage [list_stages] {
    foreach out [stage_outs $stage] {
        if {[string match *.parquet $out]} {
            lappend parquets [file rootname $out]
            puts $pqlf ${out}.json
        }
    }
}
close $pqlf

stage schema -items [lsort $parquets] {
    cmd python run.py --rust pq-info -o {${item}.json} {${item}.parquet}
    dep {${item}.parquet}
    out -nocache {${item}.json}
}
