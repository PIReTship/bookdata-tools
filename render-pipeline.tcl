# Render the DVC pipeline from source files.
package require huddle
package require yaml

namespace eval dvcpipe {
    set stack {}

    proc init {name} {
        set ::dvcpipe::name $name
        set ::dvcpipe::current [huddle create]
        set ::dvcpipe::deps [huddle list]
        set ::dvcpipe::outs [huddle list]
    }

    proc push {} {
        set obj [huddle create cur $::dvcpipe::current deps $::dvcpipe::deps outs $::dvcpipe::outs]
        lappend ::dvcpipe::stack $obj
    }
}
namespace eval dvcstage {
    namespace export cmd
    namespace export wdir
    namespace export deps
    namespace export outs

    proc cmd {s} {
        huddle append ::dvcstage::current cmd [huddle string $s]
    }
    proc wdir {s} {
        huddle append ::dvcstage::current wdir [huddle string $s]
    }

    proc dep args {
        set rl 0
        foreach a $args {
            if {[string equal $a -l]} {
                set rl 1
            } else {
                if $rl {
                    foreach e $a {
                        huddle append dl [huddle string $e]
                    }
                } else {
                    huddle append dl [huddle string $a]
                }
                set rl 0
            }
        }
        huddle append ::dvcstage::current deps $dl
    }

    proc outs args {
        set dl [huddle list]
        set rl 0
        foreach a $args {
            if {[string equal $a -l]} {
                set rl 1
            } else {
                if $rl {
                    foreach e $a {
                        huddle append dl [huddle string $e]
                    }
                } else {
                    huddle append dl [huddle string $a]
                }
                set rl 0
            }
        }
        huddle append ::dvcstage::current outs $dl
    }
}

proc stage {name block} {
    set ::dvcstage::current [huddle create]
    namespace eval dvcstage $block
    huddle append ::dvcpipe::stages $name $::dvcstage::current
}

proc pipeline {fn block} {
    set ::dvcpipe::stages [huddle create]
    uplevel $block

    set pipeline [huddle create stages $::dvcpipe::stages]

    puts "writing $fn"
    set fp [open $fn w]
    puts $fp [::yaml::huddle2yaml $pipeline 2 250]
    close $fp
}
