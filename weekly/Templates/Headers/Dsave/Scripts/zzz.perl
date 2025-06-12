#!/usr/bin/perl 

$input = `ls /data/mta/Script/Weekly/Temprate/Headers/*`;
@list  = split(/\n+/, $input);
foreach $ent (@list){
    $out = lc($ent);
    @atemp = split(/headers\//, $out);
    open(OUT,">$atemp[1]");
    close(OUT)

}

