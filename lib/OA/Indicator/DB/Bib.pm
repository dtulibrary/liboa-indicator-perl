package OA::Indicator::DB::Bib;

use strict;
use warnings;
use OA::Indicator::DB::DOAJ;
use OA::Indicator::DB::BFI;
use OA::Indicator::DB::Romeo;

our $VERSION = '1.0';

sub new
{
    my ($class, $db, $oai) = @_;
    my $self = {};

    $self->{'db'}    = $db;
    $self->{'oai'}   = $oai;
    $self->{'doaj'}  = new OA::Indicator::DB::DOAJ ($db, $oai);
    $self->{'bfi'}   = new OA::Indicator::DB::BFI ($db, $oai);
    $self->{'romeo'} = new OA::Indicator::DB::Romeo ($db, $oai);
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
                   pubyear               text,
                   type                  text,
                   level                 text,
                   review                text,
                   dedupkey              text,
                   issn                  text,
                   eissn                 text,
                   research_area         text,
                   bfi_research_area     text,
                   pub_research_area     text,
                   doi                   text,
                   title                 text,
                   first_author          text,
                   first_author_pos      integer,
                   mods                  text,
                   original_xml          text,
                   scoped_type           integer,
                   scoped_level          integer,
                   scoped_review         integer,
                   scoped                integer,
                   screened_issn         integer,
                   screened_year         integer,
                   screened              integer,
                   fulltext_link         integer,
                   fulltext_link_oa      integer,
                   fulltext_downloaded   integer,
                   fulltext_verified     integer,
                   fulltext_pdf          integer,
                   doaj_issn             text,
                   bfi_level             integer,
                   bfi_class             text,
                   romeo_color           text,
                   romeo_issn            text,
                   class                 text,
                   class_reasons         text,
                   pub_class             text,
                   pub_class_reasons     text
               )');
    $db->sql ('create index if not exists records_source_id           on records (source_id)');
    $db->sql ('create index if not exists records_source              on records (source)');
    $db->sql ('create index if not exists records_dedupkey            on records (dedupkey)');
    $db->sql ('create index if not exists records_scoped_type         on records (scoped_type)');
    $db->sql ('create index if not exists records_scoped_level        on records (scoped_level)');
    $db->sql ('create index if not exists records_scoped_review       on records (scoped_review)');
    $db->sql ('create index if not exists records_scoped              on records (scoped)');
    $db->sql ('create index if not exists records_screened_issn       on records (screened_issn)');
    $db->sql ('create index if not exists records_screened_year       on records (screened_year)');
    $db->sql ('create index if not exists records_screened            on records (screened)');
    $db->sql ('create index if not exists records_fulltext_link       on records (fulltext_link)');
    $db->sql ('create index if not exists records_fulltext_downloaded on records (fulltext_downloaded)');
    $db->sql ('create index if not exists records_fulltext_verified   on records (fulltext_verified)');
    $db->sql ('create index if not exists records_fulltext_pdf        on records (fulltext_pdf)');
    $db->sql ('create index if not exists records_romeo_color         on records (romeo_color)');
    $db->sql ('create index if not exists records_pub_research_area   on records (pub_research_area)');
    $db->sql ('create index if not exists records_research_area       on records (research_area)');
    $db->sql ('create index if not exists records_pub_class           on records (pub_class)');
    $db->sql ('create index if not exists records_class               on records (class)');
    $db->sql ('create index if not exists records_title               on records (title)');
}

sub load
{
    my ($self, $year) = @_;
#   Quick fix to add publication ID, bfi_class and bfi_level
    my $rs = $self->{'db'}->select ('source_id,publication_id,class,journal_level,research_area', 'bfi');
    my $rec;
    while ($rec = $self->{'db'}->next ($rs)) {
        if ($rec->{'publication_id'}) {
            $self->{'bfi_id'}{$rec->{'source_id'}} = $rec->{'publication_id'};
        } else {
            $self->{'bfi_id'}{$rec->{'source_id'}} = 0;
        }
        $self->{'bfi_class'}{$rec->{'source_id'}} = $rec->{'class'};
        $self->{'bfi_level'}{$rec->{'source_id'}} = $rec->{'journal_level'};
        $self->{'bfi_research_area'}{$rec->{'source_id'}} = $rec->{'research_area'};
    }
#   Quick fix for missing year in MODS and to get ISSNs, ... and research area, doi, title and first author
    $rs = $self->{'db'}->select ('id,year,pubyear,issn,eissn,research_area,doi,title_main,title_sub,first_author,first_author_pos', 'mxd');
    while ($rec = $self->{'db'}->next ($rs)) {
        $self->{'year'}{$rec->{'id'}} = $rec->{'year'};
        $self->{'pubyear'}{$rec->{'id'}} = $rec->{'pubyear'};
        $self->{'issn'}{$rec->{'id'}} = $rec->{'issn'};
        $self->{'eissn'}{$rec->{'id'}} = $rec->{'eissn'};
        $self->{'research_area'}{$rec->{'id'}} = $rec->{'research_area'};
        $self->{'doi'}{$rec->{'id'}} = $rec->{'doi'};
        if ($rec->{'title_sub'}) {
            $self->{'title'}{$rec->{'id'}} = $rec->{'title_main'} . ' : ' . $rec->{'title_sub'};
        } else {
            $self->{'title'}{$rec->{'id'}} = $rec->{'title_main'};
        }
        $self->{'first_author'}{$rec->{'id'}} = $rec->{'first_author'};
        $self->{'first_author_pos'}{$rec->{'id'}} = $rec->{'first_author_pos'};
        if ($rec->{'issn'}) {
            foreach my $issn (split (',', $rec->{'issn'})) {
                if ($issn =~ m/^[0-9]{7}[0-9X]$/) {
                    if ((!$self->{'doaj_issn'}{$rec->{'id'}}) && ($self->{'doaj'}->exists ($issn))) {
                        $self->{'doaj_issn'}{$rec->{'id'}} = $issn;
                    }
                    my $color = $self->{'romeo'}->color ($issn);
                    if ($self->color_id ($color->[0]) > $self->color_id ($self->{'romeo_color'}{$rec->{'id'}})) {
                        $self->{'romeo_color'}{$rec->{'id'}} = $color->[0];
                        $self->{'romeo_issn'}{$rec->{'id'}} = $issn;
                    }
                }
            }
        }
        if ($rec->{'eissn'}) {
            foreach my $issn (split (',', $rec->{'eissn'})) {
                if ($issn =~ m/^[0-9]{7}[0-9X]$/) {
                    if ((!$self->{'doaj_issn'}{$rec->{'id'}}) && ($self->{'doaj'}->exists ($issn))) {
                        $self->{'doaj_issn'}{$rec->{'id'}} = $issn;
                        last;
                    }
                    my $color = $self->{'romeo'}->color ($issn);
                    if ($self->color_id ($color->[0]) > $self->color_id ($self->{'romeo_color'}{$rec->{'id'}})) {
                        $self->{'romeo_color'}{$rec->{'id'}} = $color->[0];
                        $self->{'romeo_issn'}{$rec->{'id'}} = $issn;
                    }
                }
            }
        }
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
        foreach my $fld (qw(id stamp date status source_id year type level review dedupkey)) {
            $rec->{$fld} = shift (@fields);
        }
        if ($rec->{'status'} ne 'deleted') {
            my $id = $rec->{'id'};
            foreach my $f (qw(original_xml bfi_id bfi_class issn eissn doaj_issn romeo_color romeo_issn)) {
                $rec->{$f} = '';
            }
            $rec->{'source'} = $src;
            if (exists ($self->{'bfi_id'}{$rec->{'source_id'}})) {
                $rec->{'bfi_id'} = $self->{'bfi_id'}{$rec->{'source_id'}};
            }
            if (exists ($self->{'bfi_class'}{$rec->{'source_id'}})) {
                $rec->{'bfi_class'} = $self->{'bfi_class'}{$rec->{'source_id'}};
            }
            if (exists ($self->{'bfi_level'}{$rec->{'source_id'}})) {
                $rec->{'bfi_level'} = $self->{'bfi_level'}{$rec->{'source_id'}};
            } else {
                $rec->{'bfi_level'} = 0;
            }
            if (exists ($self->{'bfi_research_area'}{$rec->{'source_id'}})) {
                $rec->{'bfi_research_area'} = $self->{'bfi_research_area'}{$rec->{'source_id'}};
            } else {
                $rec->{'bfi_research_area'} = '';
            }
            if (!$records->{$id}{'year'}) {
                $records->{$id}{'year'} = $self->{'year'}{$id};
            }
            $rec->{'pubyear'} = $self->{'pubyear'}{$id};
            if ($self->{'issn'}{$id}) {
                $rec->{'issn'} = $self->{'issn'}{$id};
            }
            if ($self->{'eissn'}{$id}) {
                $rec->{'eissn'} = $self->{'eissn'}{$id};
            }
            if ($self->{'research_area'}{$id}) {
                $rec->{'research_area'} = $self->{'research_area'}{$id};
            }
            if ($self->{'doi'}{$id}) {
                $rec->{'doi'} = $self->{'doi'}{$id};
            }
            if ($self->{'title'}{$id}) {
                $rec->{'title'} = $self->{'title'}{$id};
            }
            if ($self->{'first_author'}{$id}) {
                $rec->{'first_author'} = $self->{'first_author'}{$id};
                $rec->{'first_author_pos'} = $self->{'first_author_pos'}{$id};
            }
            if ($self->{'doaj_issn'}{$id}) {
                $rec->{'doaj_issn'} = $self->{'doaj_issn'}{$id};
            }
            if ($self->{'romeo_color'}{$id}) {
                $rec->{'romeo_color'} = $self->{'romeo_color'}{$id};
                $rec->{'romeo_issn'} = $self->{'romeo_issn'}{$id};
            }
            $records->{$id} = $rec;
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

sub color_id
{
    my ($self, $color) = @_;

    if (!defined ($color)) {
        return (0);
    }
    my $romeo = {
        green  => 4,
        blue   => 3,
        yellow => 2,
        white  => 1,
    };
    if (exists ($romeo->{$color})) {
        return ($romeo->{$color});
    } else {
        return (0);
    }
}

1;

