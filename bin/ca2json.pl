#!/usr/bin/perl

use strict;
use File::Basename;
use Getopt::Std;

my %opts = ();
getopts('t:d:', \%opts);

my $curid = '';
my($sn, $year, $month, $day, $ed, $seq);
while (<>) {
    chomp;
    if ( /^[^<].*\.xml$/ ) {
	($sn, $year, $month, $day, $ed, $seq) = split /\//;
	my $date = "$year-$month-$day";
	my $id = join('/', $sn, $date, $ed, $seq);
	my $issue = join('/', $sn, $date, $ed);
	$ed =~ s/^ed\-//;
	$seq =~ s/^seq\-//;
	
	my $url = "http://chroniclingamerica.loc.gov/lccn/$id";
	
	if ( $curid ne $id ) {
	    if ( $curid ne '' ) {
		print "\"}\n";
	    }
	    $curid = $id;
	    print "{\n";
	    print "\"id\": \"$id\",\n";
	    print "\"series\": \"$sn\",\n";
	    print "\"issue\": \"$issue\",\n";
	    print "\"date\": \"$date\",\n";
	    print "\"ed\": \"$ed\",\n";
	    print "\"seq\": \"$seq\",\n";
	    print "\"url\": \"$url\",\n";
	    print "\"text\": \"";
	}
    
	# print "<pb n=\"$seq\"/>\n";
	next;
    }

    while ( /(<\/?[A-Za-z][^>]*>)/g ) {
	my $t = $1;
	
	if ( $t =~ /^<String / ) {
	    my($h) = ( $t =~ / HEIGHT=[\'\"]([0-9]+)/ );
	    my($w) = ( $t =~ / WIDTH=[\'\"]([0-9]+)/ );
	    my($x) = ( $t =~ / HPOS=[\'\"]([0-9]+)/ );
	    my($y) = ( $t =~ / VPOS=[\'\"]([0-9]+)/ );
	    print "<w coords=\\\"$x,$y,$w,$h\\\"/>";
	    if ( $t =~ / CONTENT=\"([^\"]+)\"/ ) {
		print jsesc($1);
	    } elsif ( $t =~ / CONTENT=\'([^\']+)\'/ ) {
		print jsesc($1);
	    }
	} elsif ( $t =~ /^<SP / ) {
	    print " ";
	} elsif ( $t =~ /^<\/TextLine>/ ) {
	    print "\n";
	} elsif ( $t =~ /^<HYP /) {
	    print "\xc2\xad";
	} elsif ( $t =~ /^<\/TextBlock>/ ) {
	    print "\n";
	}
    }
    

    # ## Some documents transcribe hyphens as the logical not character
    # s/[ ]*\xc2\xac[ ]*\n[ ]*(\S+)[ ]+/$1\n/g;
}

if ( $curid ne '' ) {
    print "\"}\n"
}

sub jsesc {
    my($s) = @_;
    $s =~ s/\\/\\\\/g;
    $s =~ s/\"/\\"/g;
    $s;
}
