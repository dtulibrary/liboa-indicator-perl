package OA::Indicator::DB::BFI;

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
    $db->sql ('create table if not exists bfi (
                   id              text primary key,
                   harvest_stamp   integer,
                   harvest_date    text,
                   source          text,
                   source_id       text,
                   publication_id  text,
                   doc_type        text,
                   doc_review      text,
                   doc_level       text,
                   class           text,
                   research_area   text,
                   journal_level   integer,
                   fraction        text,
                   point           text,
                   cooperation     text,
                   title           text,
                   lang            text,
                   issn            text,
                   eissn           text,
                   mods            text,
                   original_xml    text
              )');
    $db->sql ('create index if not exists bfi_source_id on bfi (source_id)');
    $db->sql ('create index if not exists bfi_publication_id on bfi (publication_id)');
}

sub load
{
    my ($self, $year) = @_;
    my ($fin);

    if (!open ($fin, "zcat /var/lib/oa-indicator/$year/bfi/bfi.ids.gz |")) {
        $self->{'oai'}->log ('f',  "failed to open /var/lib/oa-indicator/$year/bfi/bfi.ids.gz ($!)");
        $self->{'oai'}->log ('f',  'failed');
        return (0);
    }
    my $count = 0;
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my $rec = {};
        my @fields = split ("\t");
        foreach my $fld (qw(id harvest_stamp harvest_date status source source_id publication_id doc_type doc_review doc_level class research_area journal_level fraction point cooperation title lang issn eissn)) {
            $rec->{$fld} = shift (@fields);
        }
        $rec->{'id'} = $rec->{'source_id'};
        if ($rec->{'status'} eq 'deleted') {
            next;
        } else {
            delete ($rec->{'status'});
        }
        $rec->{'source'} = lc ($rec->{'source'});
        if ($rec->{'research_area'} eq 'HUM') {
            $rec->{'research_area'} = 'hum';
        } elsif ($rec->{'research_area'} eq 'SAM') {
            $rec->{'research_area'} = 'soc';
        } elsif ($rec->{'research_area'} eq 'SUND') {
            $rec->{'research_area'} = 'med';
        } elsif ($rec->{'research_area'} eq 'NAT/TEK') {
            $rec->{'research_area'} = 'sci';
        } else {
            $self->{'oai'}->log ('e',  'unknown research area: ' . $rec->{'research_area'});
        }
        if (!$rec->{'journal_level'}) {
            $rec->{'journal_level'} = 0;
        } else {
            if (($rec->{'journal_level'} != 1) && ($rec->{'journal_level'} != 2)) {
                $rec->{'journal_level'} = 0;
            }
        }
        $rec->{'mods'} = $rec->{'original_xml'} = '';
        $self->{'db'}->insert ('bfi', $rec);
        $count++;
    }
    close ($fin);
    $self->{'oai'}->log ('i',  "loaded $count records");
    $self->{'oai'}->log ('i',  'done');
    return (1);
}

sub pubid
{
    my ($self, $id) = @_;

    my $rs = $self->{'db'}->select ('publication_id', 'bfi', "source_id='$id'");
    my $rec;
    if ($rec = $self->{'db'}->next ($rs)) {
        return ($rec->{'publication_id'});
    } else {
        return (-1);
    }
}

sub level
{
    my ($self, $id) = @_;

    my $rs = $self->{'db'}->select ('journal_level', 'bfi', "source_id='$id'");
    my $rec;
    if ($rec = $self->{'db'}->next ($rs)) {
        return ($rec->{'journal_level'});
    } else {
        return (0);
    }
}

sub research_area
{
    my ($self, $id) = @_;

    my $rs = $self->{'db'}->select ('research_area', 'bfi', "source_id='$id'");
    my $rec;
    if ($rec = $self->{'db'}->next ($rs)) {
        return ($rec->{'research_area'});
    } else {
        return ('');
    }
}

1;

