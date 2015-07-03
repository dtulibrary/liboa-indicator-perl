package OA::Indicator::DB::Bib;

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
#   FIX - add a bunch of dates regarding the record, creation, update, OAI datestamp, harvest, local harvest
    $db->sql ('create table if not exists records (
                   id                    text primary key,
                   source_id             text,                                                                                                                
                   source                text,
                   bfi_id                text,
                   stamp                 integer,
                   date                  text,
                   status                text,
                   year                  integer,
                   type                  text,
                   level                 text,
                   review                text,
                   issn                  text,
                   eissn                 text,
                   mods                  text,
                   original_xml          text,
                   scoped_type           integer,
                   scoped_level          integer,
                   scoped_review         integer,
                   scoped                integer,
                   screened_issn         integer,
                   screened              integer,
                   fulltext_link         integer,
                   fulltext_link_oa      integer,
                   fulltext_downloaded   integer,
                   fulltext_verified     integer,
                   fulltext_pdf          integer,
                   romeo_color           text
               )');
    $db->sql ('create index if not exists records_source_id           on records (source_id)');
    $db->sql ('create index if not exists records_scoped_type         on records (scoped_type)');
    $db->sql ('create index if not exists records_scoped_level        on records (scoped_level)');
    $db->sql ('create index if not exists records_scoped_review       on records (scoped_review)');
    $db->sql ('create index if not exists records_scoped              on records (scoped)');
    $db->sql ('create index if not exists records_screened_issn       on records (screened_issn)');
    $db->sql ('create index if not exists records_screened            on records (screened)');
    $db->sql ('create index if not exists records_fulltext_link       on records (fulltext_link)');
    $db->sql ('create index if not exists records_fulltext_downloaded on records (fulltext_downloaded)');
    $db->sql ('create index if not exists records_fulltext_verified   on records (fulltext_verified)');
    $db->sql ('create index if not exists records_fulltext_pdf        on records (fulltext_pdf)');
    $db->sql ('create index if not exists records_romeo_color         on records (romeo_color)');
}

sub load
{
    my ($self, $year) = @_;

#   Quick fix to add publication ID, probably not the final version
    my $rs = $self->{'db'}->select ('id,publication_id', 'bfi', "publication_id!='0'");
    my $rec;
    while ($rec = $self->{'db'}->next ($rs)) {
        $self->{'bfi_id'}{$rec->{'id'}} = $rec->{'publication_id'};
    }
#   Quick fix for missing year in MODS
    $rs = $self->{'db'}->select ('id,year', 'mxd');
    while ($rec = $self->{'db'}->next ($rs)) {
        $self->{'year'}{$rec->{'id'}} = $rec->{'year'};
    }
    foreach my $src (qw(aau au cbs dtu itu ku ruc sdu)) {
        $self->{'oai'}->log ('i', "loading bibliographic records for $src");
        if (!$self->load_source ($year, $src)) {
            return (0);
        }
    }
    $self->{'oai'}->log ('i',  'done');
    return (0);
}

sub load_source
{
    my ($self, $year, $src) = @_;
    my ($fin);

    if (!open ($fin, "zcat /var/lib/oa-indicator/$year/bib/$src.ids.gz |")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/bib/$src.ids.gz ($!)");
        $self->{'oai'}->log ('f', 'failed');
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
        foreach my $fld (qw(id stamp date status source_id year type level review)) {
            $rec->{$fld} = shift (@fields);
        }
        $rec->{'mods'} = $rec->{'original_xml'} = '';
        if ($rec->{'status'} ne 'deleted') {
            $records->{$rec->{'id'}} = $rec;
            $count->{'rows'}++;
        }
    }
    close ($fin);
    if (!open ($fin, "zcat /var/lib/oa-indicator/$year/bib/$src.xml.gz |")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/bib/$src.xml.gz ($!)");
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
                    $self->{'oai'}->log ('f', "id does not exist: $id in $src");
                    $self->{'oai'}->log ('f', 'failed');
                    return (0);
                }
                $records->{$id}{'mods'} = $xml;
                $records->{$id}{'source'} = $src;
                if (exists ($self->{'bfi_id'}{$records->{$id}{'source_id'}})) {
                    $records->{$id}{'bfi_id'} = $self->{'bfi_id'}{$records->{$id}{'source_id'}};
                }
                if (!$records->{$id}{'year'}) {
                    $records->{$id}{'year'} = $self->{'year'}{$id};

                }
                $self->{'db'}->insert ('records', $records->{$id});
                delete ($records->{$id});
                $count->{'bib'}++;
                if (($count->{'bib'} % 10000) == 0) {
                    $self->{'oai'}->log ('i', "loaded $count->{'bib'} of $count->{'rows'}");
                }
            } else {
                $self->{'oai'}->log ('f', " could not get identifier from MODS XML:\n$xml");
                $self->{'oai'}->log ('f', 'failed');
                return (0);
            }
        }
    }
    $self->{'oai'}->log ('i', "loaded $count->{'bib'} of $count->{'rows'}");
    foreach my $id (keys (%{$records})) {
        $count->{'missing'}++;
    }
    if ($count->{'missing'}) {
        $self->{'oai'}->log ('w', "$count->{'missing'} missing MODS");
    }
    $self->{'oai'}->log ('i',  'done');
    return (1);
}

sub pubid
{
    my ($self, $id) = @_;

    my $rs = $self->{'db'}->select ('publication_id', 'bfi', "id='$id'");
    my $rec;
    if ($rec = $self->{'db'}->next ($rs)) {
        return ($rec->{'publication_id'});
    } else {
        return (-1);
    }
}

1;

