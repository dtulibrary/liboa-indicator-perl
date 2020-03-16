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
                   apc             integer,
                   apc_price       text,
                   apc_currency    text,
                   apc_url         text,
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
        for my $fld (qw(id harvest_stamp harvest_date status source_id pissn eissn license publisher title apc_price apc_currency apc_url)) {
            $rec->{$fld} = shift (@fields);
        }
        if ($rec->{'status'} ne 'deleted') {
            $rec->{'pissn'} = $self->issn_normalize ($rec->{'pissn'});
            $rec->{'eissn'} = $self->issn_normalize ($rec->{'eissn'});
            $rec->{'mods'} = $rec->{'original_xml'} = '';
            if ((!defined ($rec->{'apc_price'})) || ($rec->{'apc_price'} =~ m/^[\s\t\r\n]*$/)) {
                $rec->{'apc'} = 0;
            } else {
                $rec->{'apc'} = 1;
            }
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

    my @ISSN = ();
    foreach my $i (split (';', $issn)) {
        $i = uc ($i);
        $i =~ s/[^0-9X]//g;
        if ($i) {
            push (@ISSN, $i);
        }
    }
    if (@ISSN) {
        return (join (';', @ISSN));
    } else {
        return ('');
    }
}

sub cache
{
    my ($self) = @_;

    if (exists ($self->{'cache'})) {
        return (1);
    }
    $self->{'cache'} = {};
    my $rs = $self->{'db'}->select ('id,pissn,eissn,license,apc,apc_price,apc_currency', 'doaj');
    my $rec;
    while ($rec = $self->{'db'}->next ($rs)) {
        foreach my $fld (qw(pissn eissn)) {
            if ((!defined ($rec->{$fld})) || ($rec->{$fld} eq '')) {
                next;
            }
            foreach my $issn (split (';', $rec->{$fld})) {
                if ($issn !~ m/^[0-9]{7}[0-9X]$/) {
                    $self->{'oai'}->log ('w',  "invalid ISSN: $issn for $rec->{'id'}");
                    next;
                }
                if (exists ($self->{'cache'}{$issn})) {
                    if ($self->{'cache'}{$issn}->[0] ne $rec->{'license'}) {
                        $self->{'oai'}->log ('w',  "existing ISSN with two different license: $issn : $self->{'cache'}{$issn}->[0] != $rec->{'license'}");
                    }
                    if ($self->{'cache'}{$issn}->[2] ne $rec->{'apc'}) {
                        $self->{'oai'}->log ('w',  "existing ISSN with two different APC: $issn : $self->{'cache'}{$issn}->[2] != $rec->{'apc'}");
                    }
                } else {
                    if (($rec->{'apc'}) && ($rec->{'apc_currency'})) {
                        $rec->{'apc_price'} = $rec->{'apc_currency'} . ' ' . $rec->{'apc_price'};
                    }
                    $self->{'cache'}{$issn} = [$rec->{'license'}, $rec->{'id'}, $rec->{'apc'}, $rec->{'apc_price'}];
                }
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

sub apc
{
    my ($self, $issn) = @_;

    $self->cache ();
    $issn = $self->issn_normalize ($issn);
    if ($self->{'cache'}{$issn}) {
        return ($self->{'cache'}{$issn}->[2], $self->{'cache'}{$issn}->[3]);
    } else {
        return (undef);
    }
}

1;

