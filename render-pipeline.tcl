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

    proc _ensure_list {name} {
        set keys [huddle keys $::dvcpipe::current]
        if {[lsearch $name $keys] < 0} {
            huddle append ::dvcpipe::current $name [huddle list]
        }
    }

    proc init {name} {
        dbg_msg "initializing stage $name"
        set ::dvcpipe::name $name
        if {[array exists ::dvcpipe::current]} {
            array unset ::dvcpipe::current
        }
        array set ::dvcpipe::current {}
        set ::dvcpipe::targets [list]
    }

    proc save {} {
        if {[info exists ::dvcpipe::name]} {
            dbg_msg "saving $::dvcpipe::name"
            set obj [dict create name $::dvcpipe::name obj [array get ::dvcpipe::current] targets $::dvcpipe::targets]
            lappend ::dvcpipe::stack $obj
        }
    }

    proc restore {} {
        if {[llength $::dvcpipe::stack] > 0} {
            set obj [lindex $::dvcpipe::stack ::dvcpipe::stack end]
            set ::dvcpipe::stack [lreplace $::dvcpipe::stack end-1 end]
            set ::dvcpipe::name [dict get $obj name]
            dbg_msg "restoring $::dvcpipe::name"
            if {[array exists ::dvcpipe::current]} {
                array unset ::dvcpipe::current
            }
            array set ::dvcpipe::current [dict get $obj obj]
            set ::dvcpipe::targets [dict get $obj targets]
        }
    }

    proc set_field {key val} {
        dbg_msg "key: $key"
        dbg_msg "val: $val"
        array set ::dvcpipe::current [list $key $val]
    }

    proc push_field {key val} {
        if {[info exists ::dvcpipe::current($key)]} {
            huddle append ::dvcpipe::current($key) $val
        } else {
            array set ::dvcpipe::current [list $key [huddle list $val]]
        }
    }

    proc finish {} {
        dbg_msg "finishing $::dvcpipe::name"
        set var [huddle create]

        foreach v {cmd wdir deps outs metrics} {
            if {[info exists ::dvcpipe::current($v)]} {
                huddle append var $v $::dvcpipe::current($v)
            }
        }

        if {[llength $::dvcpipe::targets] > 0} {
            set tgts [huddle list]
            foreach t $::dvcpipe::targets {
                huddle append tgts [huddle string $t]
            }
            set var [huddle create foreach $tgts do $var]
        }

        unset ::dvcpipe::name
        unset ::dvcpipe::current
        unset ::dvcpipe::targets

        return $var
    }
}

namespace eval dvcstage {
    namespace export target
    namespace export cmd
    namespace export wdir
    namespace export dep
    namespace export out

    proc mkout {nocache dep} {
        if {$nocache} {
            return [huddle create $dep [huddle create cache false]]
        } else {
            return [huddle string $dep]
        }
    }

    proc target {tgt} {
        dbg_msg "task $::dvcpipe::name has subtarget $tgt"
        lappend ::dvcpipe::targets $tgt
    }

    proc cmd {s} {
        ::dvcpipe::set_field cmd [huddle string $s]
    }
    proc wdir {s} {
        ::dvcpipe::set_field wdir [huddle string $s]
    }

    proc dep args {
        set rl 0
        foreach a $args {
            if {[string equal $a -l]} {
                set rl 1
            } else {
                if $rl {
                    foreach e $a {
                        ::dvcpipe::push_field deps [huddle string $e]
                    }
                } else {
                    ::dvcpipe::push_field deps [huddle string $a]
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
                        ::dvcpipe::push_field outs [mkout $nocache $e]
                    }
                } else {
                    ::dvcpipe::push_field outs [mkout $nocache $a]
                }
                set rl 0
                set nocache 0
            }
        }
    }

    proc metric args {
        set nocache 0
        foreach a $args {
            if {[string equal $a -nocache]} {
                set nocache 1
            } else {
                ::dvcpipe::push_field metrics [mkout $nocache $a]
                set nocache 0
            }
        }
    }
}

proc stage {name block} {
    ::dvcpipe::save
    ::dvcpipe::init $name
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

    dbg_msg $pipeline

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
