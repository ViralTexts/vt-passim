#!/usr/bin/perl

use strict;

while (<>) {
    s/<\?xml[^>]+>//g;
    s/\\/\\\\/g;
    s/\"/\\"/g;
    s/>([^<]+)</>\"$1\"</g;
    s/<Record>/\{/g;
    s/<\/Record>/\}\n/g;
    1 while s/(<[A-Za-z][^>]*>)(<[A-Za-z][^>]*>)/$1\{$2/g;
    1 while s/(<\/[A-Za-z][^>]*>)(<\/[A-Za-z][^>]*>)/$1\}$2/g;
    s/<([A-Za-z][^>]*)>/\"$1\": /g;
    s/<\/[A-Za-z][^>]*>/,\n/g;
    s/,\s*\}/\}/gs;
    s/([ ]{4,})/\n$1/g;
    print;
}
