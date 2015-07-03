package OA::Indicator::DB::DOAJ;

use strict;
use warnings;

our $VERSION = '1.0';

sub new
{
    my ($class, $db, $oai) = @_;
    my $self = {};

    $self->{'db'} = $db;
    $self->{'oai'} = $oai;
    return (bless ($self, $class));
}

sub create
{   
    my ($self) = @_;

    my $db = $self->{'db'};
    $db->sql ('create table if not exists doaj (
                   id              text primary key,
                   harvest_stamp   integer,
                   harvest_date    text,
                   source_id       text,
                   pissn           text,
                   eissn           text,
                   license         text,
                   publisher       text,
                   title           text,
                   mods            text,
                   original_xml    text
              )');
    $db->sql ('create index if not exists doaj_source_id on doaj (source_id)');
    $db->sql ('create index if not exists doaj_pissn on doaj (pissn)');
    $db->sql ('create index if not exists doaj_eissn on doaj (eissn)');
}

sub load
{
    my ($self, $year) = @_;
    my ($fin);

    if (!open ($fin, "zcat /var/lib/oa-indicator/$year/doaj/doaj.ids.gz |")) {
        $self->{'oai'}->log ('f',  "failed to open /var/lib/oa-indicator/$year/doaj/doaj.ids.gz ($!)");
        $self->{'oai'}->log ('f',  'failed');
        return (0);
    }
    my $count = {};
    my $records = {};
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my $rec = {};
        my @fields = split ("\t");
        for my $fld (qw(id harvest_stamp harvest_date status source_id pissn eissn license publisher title)) {
            $rec->{$fld} = shift (@fields);
        }
        if ($rec->{'status'} ne 'deleted') {
            $rec->{'pissn'} = $self->issn_normalize ($rec->{'pissn'});
            $rec->{'eissn'} = $self->issn_normalize ($rec->{'eissn'});
            $rec->{'mods'} = $rec->{'original_xml'} = '';
            delete ($rec->{'status'});
            $records->{$rec->{'id'}} = $rec;
            $count->{'rows'}++;
        }
    }
    close ($fin);
    if (!open ($fin, "zcat /var/lib/oa-indicator/$year/doaj/doaj.xml.gz |")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/doaj/doaj.xml.gz ($!)");
        $self->{'oai'}->log ('f', 'failed');
        return (0);
    }
    my $buf = '';
    while (<$fin>) {
        chomp;
        $buf .= $_;
        $buf =~ s/^[\s\t\r\n]*<\?xml .*?\?>//;
        $buf =~ s/^[\s\t\r\n]*<records>//;
        $buf =~ s/^[\s\t\r\n]+//;
        while ($buf =~ s/^.*?(<mods .*?<\/mods>)//s) {
            my $xml = $1;
            if ($xml =~ m/<identifier [^>]*type="ds.dtic.dk:id:pub:dads:recordid">([^<]+)<\/identifier>/) {
                my $id = $1;
                if (!exists ($records->{$id})) {
                    $self->{'oai'}->log ('f', "id does not exist: $id in doaj");
                    $self->{'oai'}->log ('f', 'failed');
                    return (0);
                }
                $records->{$id}{'mods'} = $xml;
                $self->{'db'}->insert ('doaj', $records->{$id});
                delete ($records->{$id});
                $count->{'doaj'}++;
                if (($count->{'doaj'} % 1000) == 0) {
                    $self->{'oai'}->log ('i', "loaded $count->{'doaj'} of $count->{'rows'}");
                }
            } else {
                $self->{'oai'}->log ('f', " could not get identifier from MODS XML:\n$xml");
                $self->{'oai'}->log ('f', 'failed');
                return (0);
            }
        }
    }
    $self->{'oai'}->log ('i', "loaded $count->{'doaj'} of $count->{'rows'}");
    foreach my $id (keys (%{$records})) {
        $count->{'missing'}++;
    }
    if ($count->{'missing'}) {
        $self->{'oai'}->log ('w', "$count->{'missing'} missing MODS");
    }
    $self->{'oai'}->log ('i',  'done');
    return (1);
}

sub issn_normalize
{
    my ($self, $issn) = @_;

    $issn = uc ($issn);
    $issn =~ s/[^0-9X]//g;
    return ($issn);
}

sub cache
{
    my ($self, $issn) = @_;

    if (exists ($self->{'cache'})) {
        return (1);
    }
    $self->{'cache'} = {};
    my $rs = $self->{'db'}->select ('id,pissn,eissn,license', 'doaj');
    my $rec;
    while ($rec = $self->{'db'}->next ($rs)) {
        foreach my $fld (qw(pissn eissn)) {
            if (exists ($self->{'cache'}{$rec->{$fld}})) {
                if ($self->{'cache'}{$rec->{$fld}}->[0] ne $rec->{'license'}) {
                    $self->{'oai'}->log ('w',  "existing ISSN with two different license: $rec->{$fld} : $self->{'cache'}{$rec->{$fld}}->[0] != $rec->{'license'}");
                }
            } else {
                $self->{'cache'}{$rec->{$fld}} = [$rec->{'license'}, $rec->{'id'}];
            }
        }
    }
}

sub exists
{
    my ($self, $issn) = @_;

    $self->cache ();
    $issn = $self->issn_normalize ($issn);
    if ($self->{'cache'}{$issn}) {
        return (1);
    } else {
        return (0);
    }
}

sub id
{
    my ($self, $issn) = @_;

    $self->cache ();
    $issn = $self->issn_normalize ($issn);
    if ($self->{'cache'}{$issn}) {
        return ($self->{'cache'}{$issn}->[1]);
    } else {
        return (undef);
    }
}

sub license
{
    my ($self, $issn) = @_;

    $self->cache ();
    $issn = $self->issn_normalize ($issn);
    if ($self->{'cache'}{$issn}) {
        return ($self->{'cache'}{$issn}->[0]);
    } else {
        return (undef);
    }
}

1;

