# Render the DVC pipeline from source files.
package require huddle
package require yaml
package require cmdline

set _show_dbg 0

proc dbg_msg {msg} {
    if {$::_show_dbg} {
        puts stderr "D: $msg"
    }
}

proc info_msg {msg} {
    puts stderr "I: $msg"
}

namespace eval dvcpipe {
    set stages [huddle false]
    set stack {}

    proc init {name} {
        set ::dvcpipe::name $name
        set ::dvcpipe::current [huddle create]
        set ::dvcpipe::deps [huddle list]
        set ::dvcpipe::outs [huddle list]
    }

    proc save {} {
        if {[info exists ::dvcpipe::name]} {
            dbg_msg "saving $::dvcpipe::name"
            set obj [huddle create name [huddle string $::dvcpipe::name] cur $::dvcpipe::current deps $::dvcpipe::deps outs $::dvcpipe::outs]
            lappend ::dvcpipe::stack $obj
        }
    }

    proc restore {} {
        if {[llength $::dvcpipe::stack] > 0} {
            set obj [lindex $::dvcpipe::stack ::dvcpipe::stack end]
            set ::dvcpipe::stack [lreplace $::dvcpipe::stack end-1 end]
            set ::dvcpipe::name [huddle get $obj name]
            dbg_msg "restoring $::dvcpipe::name"
            set ::dvcpipe::current [huddle get $obj cur]
            set ::dvcpipe::deps [huddle get $obj deps]
            set ::dvcpipe::outs [huddle get $obj outs]
        }
    }

    proc finish {} {
        dbg_msg "finishing $::dvcpipe::name"
        if {[huddle llength $::dvcpipe::deps] > 0} {
            huddle append ::dvcpipe::current deps $::dvcpipe::deps
        }
        if {[huddle llength $::dvcpipe::outs] > 0} {
            huddle append ::dvcpipe::current outs $::dvcpipe::outs
        }
        set var $::dvcpipe::current
        unset ::dvcpipe::name
        unset ::dvcpipe::current
        unset ::dvcpipe::deps
        unset ::dvcpipe::outs
        return $var
    }
}

namespace eval dvcstage {
    namespace export cmd
    namespace export wdir
    namespace export dep
    namespace export out

    proc mkdep {nocache dep} {
        if {$nocache} {
            return [huddle create $dep [huddle create cache false]]
        } else {
            return [huddle string $dep]
        }
    }

    proc cmd {s} {
        huddle append ::dvcpipe::current cmd [huddle string $s]
    }
    proc wdir {s} {
        huddle append ::dvcpipe::current wdir [huddle string $s]
    }

    proc dep args {
        set rl 0
        foreach a $args {
            if {[string equal $a -l]} {
                set rl 1
            } else {
                if $rl {
                    foreach e $a {
                        huddle append ::dvcpipe::deps [huddle string $e]
                    }
                } else {
                    huddle append ::dvcpipe::deps [huddle string $a]
                }
                set rl 0
            }
        }
    }

    proc out args {
        set rl 0
        set nocache 0
        foreach a $args {
            if {[string equal $a -l]} {
                set rl 1
            } elseif {[string equal $a -nocache]} {
                set nocache 1
            } else {
                if $rl {
                    foreach e $a {
                        huddle append ::dvcpipe::outs [mkdep $nocache $e]
                    }
                } else {
                    huddle append ::dvcpipe::outs [mkdep $nocache $a]
                }
                set rl 0
                set nocache 0
            }
        }
    }
}

proc stage {name block} {
    ::dvcpipe::save
    ::dvcpipe::init name
    namespace eval dvcstage $block
    huddle append ::dvcpipe::stages $name [::dvcpipe::finish]
    ::dvcpipe::restore
}

proc subdir {dir} {
    info_msg "processing subdirectory $dir"
    _run_pipeline_script "$dir/pipeline.tcl" "$dir/dvc.yaml"
}

proc _run_pipeline_script {tclfn ymlfn} {
    set saved $::dvcpipe::stages
    set ::dvcpipe::stages [huddle create]

    info_msg "evaluating $tclfn"
    uplevel "source \"$tclfn\""

    set stages $::dvcpipe::stages
    set ::dvcpipe::stages $saved

    set pipeline [huddle create stages $stages]

    info_msg "writing $ymlfn with [llength [huddle keys $stages]] stages"
    if {$::_cmd_params(out)} {
        puts [::yaml::huddle2yaml $pipeline 2 250]
    } else {
        set fp [open $ymlfn w]
        puts $fp [::yaml::huddle2yaml $pipeline 2 250]
        close $fp
    }
}

array set _cmd_params [::cmdline::getoptions argv {
    {v          "verbose logging output"}
    {out          "write to stdout instead of file"}
} {: [options]}]
set _show_dbg $_cmd_params(v)
dbg_msg "finished option parsing"

_run_pipeline_script "pipeline.tcl" "dvc.yaml"
