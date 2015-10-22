#!/usr/bin/perl

use strict;
use File::Basename;
use Getopt::Std;

my %opts = ();
getopts('t:d:', \%opts);

my $lastStart = -1;
my $lastLine = -20;
my $inArticle = 0;
my $inText = 0;
my $seenText = 0;
my($series, $issue, $date, $stitle);
while (<>) {
    s/^\s+//;
    s/\r\n/\n/;
    if ( /^<issue>/ or /^<\?xml/ ) {
	if ( $inArticle ) {
	    if ( $inText ) {
		print "\"";
	    } elsif ( !$seenText ) {
		print "\"text\": \"\"\n";
	    }
	    print "}\n";
	    $inArticle = 0;
	    $inText = 0;
	    $seenText =  0;
	}
    }
    if ( /^<article>/ ) {
	if ( $inArticle ) {
	    if ( $inText ) {
		print "\"";
	    } elsif ( !$seenText ) {
		print "\"text\": \"\"\n";
	    }
	    $inText = 0;
	    print "}\n";
	}
	$inArticle = 1;
	$seenText = 0;
	print "{\n";
	print sfield("issue", $issue), ",\n";
	print sfield("series", $series), ",\n";
	print sfield("date", $date), ",\n";
    } elsif ( /^<\/article>/ ) {
	if ( !$seenText ) {
	    print "\"text\": \"\"\n";
	}
	$seenText = 0;
	$inArticle = 0;
	print "}\n";
    } elsif ( /^<pf>(\d{4})(\d\d)(\d\d)<\/pf>/ ) {
	$date = "$1-$2-$3";
    } elsif ( /^<titleAbbreviation>([^<]+)<\/titleAbbreviation>/ ) {
	$series = $1;
    } elsif ( /^<lccn>([^<]+)<\/lccn>/ ) {
	$series = $1;
    } elsif ( /^<id>([^<]+)<\/id>/ ) {
	if ( $inArticle ) {
	    print sfield("id", $1), ",\n";
	} else {
	    $issue = $1;
	}
    } elsif ( /^<ti>(.+)<\/ti>/ ) {
	print sfield("title", $1), ",\n";
    } elsif ( /^<altSource>(.+)<\/altSource>/ ) {
	print sfield("altSource", $1), ",\n";
    } elsif ( /^<ct>(.+)<\/ct>/ ) {
	print sfield("category", $1), ",\n";
    } elsif ( /<text>\s*$/ ) {
	print "\"text\": \"";
	$inText = 1;
	$lastStart = -1;
	$lastLine = -20;
    } elsif ( /^<\/text>/ ) {
	if ( !$inText ) {
	    print "\"text\": \"";
	}
	print "\"\n";
	$inText = 0;
	$seenText = 1;
    } elsif ( $inText ) {
	s/([^\s<>]+)\-<\/wd>/$1\xc2\xad<\/wd>/g;
	s/<\/wd>\s*//g;
	s/\0+//g;		# busted files
	my $sep = " ";
	if ( /<wd pos=\"(\d+),(\d+),/ ) {
	    my $start = $1;
	    my $vert = $2;
	    if ( $lastStart < 0 or $lastLine < 0 ) {
		$sep = '';
	    } elsif ( $start < $lastStart or ( $vert > ($lastLine + 20) ) ) {
		$sep = "\n";
	    }
	    $lastStart = $start;
	    $lastLine = $vert;
	}
	s/<wd pos=\"(\d+),(\d+),(\d+),(\d+)\">/$sep<w coords=\"$1,$2,${ \($3 - $1) },${\($4 -$2)}\"\/>/g;
	print jsesc($_);
    }
}

sub sfield {
    my($k, $v) = @_;
    "\"$k\": \"" . jsesc($v) . "\"";
}

sub jsesc {
    my($s) = @_;
    $s =~ s/\\/\\\\/g;
    $s =~ s/\"/\\"/g;
    $s;
}
