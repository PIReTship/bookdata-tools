#!/usr/bin/env perl

use v5.32;
use utf8;
use warnings;
use File::Path qw(make_path);

my $outdir = $ENV{QUARTO_PROJECT_OUTPUT_DIR};
$outdir = "_site" if (!defined $outdir);
my @pages = glob "data/*.qmd";
my $n = @pages;
print "scanning $n doc pages, writing to $outdir\n";

make_path($outdir) or die "$outdir: $!" if ! -d $outdir;

open(my $ofh, ">$outdir/files.json") or die "$outdir/files.json: $!";
print $ofh "{\n";
my $first = 1;

foreach my $page (@pages) {
    open(my $fh, "<$page") or die "$page: $!";
    while (<$fh>) {
        if (m/^:::\s*\{.*file="([^"]+)".*\}/) {
            print "$page: $1\n";
            print $ofh ",\n" if !$first;
            print $ofh "\"$1\": {\"page\": \"$page\"}";
            $first = 0;
        }
    }
    close $fh;
}

print $ofh "\n}\n";
close $ofh;
