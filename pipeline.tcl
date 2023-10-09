namespace import ::plumber::list_stages
namespace import ::plumber::stage_*

source config.tcl

# cmd alias to run book data commands
proc ::bdcmd args {
    namespace import plumber::dsl::stage::*
    cmd cargo run --release -- {*}$args
}

subdir loc-mds
subdir openlibrary
subdir viaf
subdir az2014
subdir az2018
subdir bx
subdir goodreads
subdir book-links

stage ClusterStats {
    cmd quarto render ClusterStats.qmd
    dep ClusterStats.qmd
    dep book-links/cluster-stats.parquet
    out -nocache ClusterStats.ipynb
    out ClusterStats.html
    out ClusterStats_files
}

stage LinkageStats {
    cmd quarto render LinkageStats.qmd
    dep LinkageStats.qmd
    dep book-links/gender-stats.csv
    out LinkageStats.html
    out LinkageStats_files
    out -metric book-coverage.json
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
    bdcmd pq-info -o {${item}.json} {${item}.parquet}
    dep {${item}.parquet}
    out -nocache {${item}.json}
}
