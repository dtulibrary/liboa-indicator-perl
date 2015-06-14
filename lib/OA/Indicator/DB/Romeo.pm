package OA::Indicator::DB::Romeo;

use strict;
use warnings;

our $VERSION = '1.0';

sub new
{
    my ($class, $db) = @_;
    my $self = {};

    $self->{'db'} = $db;
    return (bless ($self, $class));
}

sub create
{   
    my ($self) = @_;

    my $db = $self->{'db'};
    $db->sql ('create table if not exists romeo (
                   id              integer primary key,
                   issn            text,                                                                                                                
                   publisher_id    integer,
                   color           text,
                   mods            text,
                   original_xml    text
              )');
    $db->sql ('create index if not exists romeo_issn on romeo (issn)');
}

sub load
{
    my ($self, $year) = @_;
    my ($fin);

    if (!open ($fin, "zcat /var/lib/oa-indicator/$year/romeo/romeo.ids.gz |")) {
        die ("fatal: failed to open /var/lib/oa-indicator/$year/romeo/romeo.ids.gz ($!)");
    }
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my $rec = {};
        ($rec->{'issn'}, $rec->{'publisher_id'}, $rec->{'color'}) = split ("\t");
        $rec->{'issn'} = $self->issn_normalize ($rec->{'issn'});
        $rec->{'mods'} = $rec->{'original_xml'} = '';
        $self->{'db'}->insert ('romeo', $rec);
    }
    close ($fin);
}

sub issn_normalize
{
    my ($self, $issn) = @_;

    $issn = uc ($issn);
    $issn =~ s/[^0-9X]//g;
    return ($issn);
}

sub color
{
    my ($self, $issn) = @_;

    $issn = $self->issn_normalize ($issn);
    if ($self->{'cache'}{$issn}) {
        return (@{$self->{'cache'}{$issn}});
    }
    my $rs = $self->{'db'}->select ('id,color', 'romeo', "issn='$issn'");
    my $rec;
    if ($rec = $self->{'db'}->next ($rs)) {
        $self->{'cache'}{$issn} = [$rec->{'color'}, $rec->{'id'}];
        return ($rec->{'color'}, $rec->{'id'});
    } else {
        $self->{'cache'}{$issn} = ['none', 0];
        return ('none', 0);
    }
#   FIX - add clearing of cache if needed or pre-load the cache based on performance
}

1;

