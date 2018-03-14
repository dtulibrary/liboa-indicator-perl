package OA::Indicator::DB::JWlep;

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
    $db->sql ('create table if not exists jwlep (
                   id              text primary key,
                   harvest_stamp   integer,
                   harvest_date    text,
                   pissn           text,
                   eissn           text,
                   title           text,
                   publisher       text,
                   embargo         text,
                   url             text,
                   usage_records   integer,
                   usage_effective integer,
                   usage_pissn     integer,
                   usage_eissn     integer,
                   mods            text,
                   original_xml    text
              )');
    $db->sql ('create index if not exists jwlep_pissn on jwlep (pissn)');
    $db->sql ('create index if not exists jwlep_eissn on jwlep (eissn)');
    $db->sql ('create index if not exists jwlep_title on jwlep (title)');
}

sub load
{
    my ($self, $year) = @_;
    my ($fin);

    if (!open ($fin, "zcat /var/lib/oa-indicator/$year/jwlep/jwlep.ids.gz |")) {
        $self->{'oai'}->log ('f',  "failed to open /var/lib/oa-indicator/$year/jwlep/jwlep.ids.gz ($!)");
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
        for my $fld (qw(id harvest_stamp harvest_date status pissn eissn title publisher embargo url)) {
            $rec->{$fld} = shift (@fields);
        }
        if ($rec->{'status'} ne 'deleted') {
            $rec->{'pissn'} = $self->issn_normalize ($rec->{'pissn'});
            $rec->{'eissn'} = $self->issn_normalize ($rec->{'eissn'});
            $rec->{'mods'} = $rec->{'original_xml'} = '';
            $rec->{'usage_records'} = $rec->{'usage_effective'} = $rec->{'usage_pissn'} = $rec->{'usage_eissn'} = 0;
            delete ($rec->{'status'});
            $records->{$rec->{'id'}} = $rec;
            $count->{'rows'}++;
        }
    }
    close ($fin);
    if (!open ($fin, "zcat /var/lib/oa-indicator/$year/jwlep/jwlep.xml.gz |")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/jwlep/jwlep.xml.gz ($!)");
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
                    $self->{'oai'}->log ('f', "id does not exist: $id in JWlep");
                    $self->{'oai'}->log ('f', 'failed');
                    return (0);
                }
                $records->{$id}{'mods'} = $xml;
                $self->{'db'}->insert ('jwlep', $records->{$id});
                delete ($records->{$id});
                $count->{'jwlep'}++;
                if (($count->{'jwlep'} % 1000) == 0) {
                    $self->{'oai'}->log ('i', "loaded $count->{'jwlep'} of $count->{'rows'}");
                }
            } else {
                $self->{'oai'}->log ('f', " could not get identifier from MODS XML:\n$xml");
                $self->{'oai'}->log ('f', 'failed');
                return (0);
            }
        }
    }
    $self->{'oai'}->log ('i', "loaded $count->{'jwlep'} of $count->{'rows'}");
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

sub valid
{
    my ($self, $issn) = @_;

    $issn = $self->issn_normalize ($issn);
    my $rs = $self->{'db'}->select ('embargo', 'jwlep', "pissn='$issn' or eissn='$issn'");
    my $rc;
    if ($rc = $self->{'db'}->next ($rs)) {
        return ($rc->{'embargo'});
    } else {
        return (0);
    }
}

sub usage_records
{   
    my ($self, $issnlist, $reclass) = @_;

    my $ISSN = {};
    foreach my $i (split (',', $issnlist)) {
        $ISSN->{$i} = 0;
    }
    my $done = {};
    foreach my $issn (keys (%{$ISSN})) {
        my $rs = $self->{'db'}->select ('id,pissn,eissn,usage_records,usage_effective,usage_pissn,usage_eissn', 'jwlep', "pissn='$issn' or eissn='$issn'");
        my $rc;
        my @rec = ();
        while ($rc = $self->{'db'}->next ($rs)) {
            if (!$done->{$rc->{'id'}}) {
                $done->{$rc->{'id'}} = 1;
                $rc->{'usage_records'}++;
                if ($reclass) {
                    $rc->{'usage_effective'}++;
                }
            }
            if ($issn eq $rc->{'pissn'}) {
                $rc->{'usage_pissn'}++;
            }
            if ($issn eq $rc->{'eissn'}) {
                $rc->{'usage_eissn'}++;
            }
            push (@rec, $rc);
        }
        foreach my $rc (@rec) {
            $self->{'db'}->update ('jwlep', 'id', $rc);
            $self->{'db'}->commit ();
        }
    }
}

1;

