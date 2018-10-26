package OA::Indicator::DB::MXD;

use strict;
use warnings;

our $VERSION = '1.0';

sub new
{
    my ($class, $db, $oai) = @_;
    my $self = {};

    $self->{'db'} = $db;
    $self->{'oai'} = $oai;
    $self->{'cache'} = {};
    $self->{'cacheids'} = [];
    return (bless ($self, $class));
}

sub create
{
    my ($self) = @_;

    my $db = $self->{'db'};
    $db->sql ('create table if not exists mxd (
                   id               text primary key,
                   source           text,
                   source_id        text,
                   stamp            integer,
                   date             text,
                   oai_harvest      text,
                   oai_datestamp    text,
                   year             integer,
                   pubyear          text,
                   doc_type         text,
                   doc_review       text,
                   doc_level        text,
                   research_area    text,
                   jno              integer,
                   pno              integer,
                   jtitle           text,
                   jtitle_alt       text,
                   series           text,
                   issn             text,
                   eissn            text,
                   doi              text,
                   title_main       text,
                   title_sub        text,
                   first_author     text,
                   first_author_pos integer,
                   original_xml     text
              )');
    $db->sql ('create index if not exists mxd_source_id on mxd (source_id)');
    $db->sql ('create table if not exists mxdft (
                   id              integer primary key,
                   dads_id         text,
                   source          text,
                   source_id       text,
                   version         text,
                   type            text,
                   access          text,
                   license         text,
                   embargo_start   text,
                   embargo_end     text,
                   url             text
              )');
    $db->sql ('create index if not exists mxdft_dads_id on mxdft (dads_id)');
    $db->sql ('create index if not exists mxdft_source_id on mxdft (source_id)');
    $db->sql ('create table if not exists mxd_person (
                   id              integer primary key,
                   dads_id         text,
                   source          text,
                   source_id       text,
                   position        integer,
                   role            text,
                   firstname       text,
                   lastname        text,
                   email           text,
                   pid             text,
                   pid_type        text,
                   pid_source      text
              )');
    $db->sql ('create index if not exists mxd_person_dads_id on mxd_person (dads_id)');
    $db->sql ('create index if not exists mxd_person_source_id on mxd_person (source_id)');
}

sub load
{
    my ($self, $year) = @_;

    foreach my $src (qw(aau au cbs dtu itu ku ruc sdu)) {
        $self->{'oai'}->log ('i',  "loading $src");
        if (!$self->load_source ($year, $src)) {
            return (0);
        }
    }
    $self->{'oai'}->log ('i',  'done');
    return (1);
}

sub load_source
{
    my ($self, $year, $src) = @_;
    my ($fin);

    my $issn = {};
    if (!open ($fin, "/var/lib/oa-indicator/$year/mxd/$src.issn")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/mxd/$src.issn ($!)");
        $self->{'oai'}->log ('f', 'failed');
        return (0);
    }
    my $count = 0;
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my ($id, $type, $i) = split ("\t");
        $i = uc ($i);
        $i =~ s/[^0-9X]//g;
        $issn->{$id}{$type}{$i} = 1;
        $count++;
    }
    close ($fin);
    $self->{'oai'}->log ('i',  "loaded $count issn");
    my $first_author = {};
#   loading first local author
    if (!open ($fin, "/var/lib/oa-indicator/$year/mxd/$src.persons")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/mxd/$src.persons ($!)");
        $self->{'oai'}->log ('f', 'failed');
        return (0);
    }
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my $rec = {};
        my @fields = split ("\t");
        foreach my $fld (qw(dads_id position role firstname lastname email pid pid_type pid_source)) {
            $rec->{$fld} = shift (@fields);
        }
        if (!$rec->{'position'}) {
            $rec->{'position'} = 0;
        }
        if ((defined ($rec->{'pid_type'})) && ($rec->{'pid_type'} eq 'loc_per') && (!exists ($first_author->{$rec->{'dads_id'}}))) {
            $first_author->{$rec->{'dads_id'}}{'name'} = $rec->{'lastname'} . ', ' . $rec->{'firstname'};
            $first_author->{$rec->{'dads_id'}}{'pos'} = $rec->{'position'};
        }
    }
    close ($fin);
#   loading primary fields
    my $idmap = {};
    my $records = {};
    if (!open ($fin, "/var/lib/oa-indicator/$year/mxd/$src.ids")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/mxd/$src.ids ($!)");
        $self->{'oai'}->log ('f', 'failed');
        return (0);
    }
    $count = {ft => 0, 'ft-no-url' => 0};
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my $rec = {};
        my @fields = split ("\t");
        foreach my $fld (qw(id stamp date oai_harvest oai_datestamp source_id year doc_type doc_level doc_review research_area jno pno jtitle jtitle_alt series doi title_main title_sub pubyear)) {
            $rec->{$fld} = shift (@fields);
        }
        $rec->{'source'} = $src;
        foreach my $f (qw(year jno pno)) {
            if (!$rec->{$f}) {
                $rec->{$f} = 0;
            }
        }
        $rec->{'original_xml'} = '';
        if (exists ($first_author->{$rec->{'id'}})) {
            $rec->{'first_author'} = $first_author->{$rec->{'id'}}{'name'};
            $rec->{'first_author_pos'} = $first_author->{$rec->{'id'}}{'pos'};
        } else {
            $rec->{'first_author'} = '';
            $rec->{'first_author_pos'} = 0;
        }
        $idmap->{$rec->{'id'}} = $rec->{'source_id'};
        $records->{$rec->{'source_id'}} = $rec;
        $count->{'rows'}++;
    }
    close ($fin);
    $self->{'oai'}->log ('i',  "loaded $count->{'rows'} ids");
    if (!open ($fin, "/var/lib/oa-indicator/$year/mxd/$src.ftx")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/mxd/$src.ftx ($!)");
        $self->{'oai'}->log ('f', 'failed');
        return (0);
    }
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my $rec = {};
        my @fields = split ("\t");
        foreach my $fld (qw(dads_id version type access license embargo_start embargo_end url)) {
            $rec->{$fld} = shift (@fields);
        }
        if ((!defined ($rec->{'url'})) || ($rec->{'url'} =~ m/^[\s\t\r\n]*$/)) {
            $count->{'ft-no-url'}++;
            next;
        }
        $rec->{'embargo_start'} =~ s/\+.*//;
        $rec->{'embargo_end'} =~ s/\+.*//;
        $rec->{'source_id'} = $idmap->{$rec->{'dads_id'}};
        $rec->{'source'} = $src;
        $self->{'db'}->insert ('mxdft', $rec);
        $count->{'ft'}++;
    }
    close ($fin);
    $self->{'oai'}->log ('i',  "loaded $count->{'ft'} fulltext links, skipped $count->{'ft-no-url'} missing URL");
#   Person records
    if (!open ($fin, "/var/lib/oa-indicator/$year/mxd/$src.persons")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/mxd/$src.persons ($!)");
        $self->{'oai'}->log ('f', 'failed');
        return (0);
    }
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my $rec = {};
        my @fields = split ("\t");
        foreach my $fld (qw(dads_id position role firstname lastname email pid pid_type pid_source)) {
            $rec->{$fld} = shift (@fields);
        }
        $rec->{'source_id'} = $idmap->{$rec->{'dads_id'}};
        $rec->{'source'} = $src;
        $self->{'db'}->insert ('mxd_person', $rec);
        $count->{'person'}++;
    }
    close ($fin);
    $self->{'oai'}->log ('i',  "loaded $count->{'person'} person records");
    if (!open ($fin, "/var/lib/oa-indicator/$year/mxd/$src.xml")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/mxd/$src.xml ($!)");
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
        while ($buf =~ s/^.*?(<mxd:ddf_doc .*?<\/mxd:ddf_doc>)//s) {
            my $xml = $1;
            if ($xml =~ m/rec_id="([^"]+)"/) {
                my $id = $1;
                if (!exists ($records->{$id})) {
                    $self->{'oai'}->log ('f',  "id does not exist: $id in $src");
                    $self->{'oai'}->log ('f',  'failed');
                    return (0);
                }
                $records->{$id}{'original_xml'} = $xml;
                my $did = $records->{$id}{'id'};
                if ($issn->{$did}) {
                    if ($issn->{$did}{'print'}) {
                        $records->{$id}{'issn'} = join (',', sort (keys (%{$issn->{$did}{'print'}})));
                    } else {
                        $records->{$id}{'issn'} = '';
                    }
                    if ($issn->{$did}{'electronic'}) {
                        $records->{$id}{'eissn'} = join (',', sort (keys (%{$issn->{$did}{'electronic'}})));
                    } else {
                        $records->{$id}{'eissn'} = '';
                    }
                }
                $self->{'db'}->insert ('mxd', $records->{$id});
                delete ($records->{$id});
                $count->{'mxd'}++;
                if (($count->{'mxd'} % 10000) == 0) {
                    $self->{'oai'}->log ('i',  "loaded $count->{'mxd'} of $count->{'rows'} MXD");
                }
            } else {
                $self->{'oai'}->log ('f',  "could not get identifier from MXD XML:\n$xml");
                $self->{'oai'}->log ('f',  'failed');
                return (0);
            }
        }
    }
    $self->{'oai'}->log ('i', "loaded $count->{'mxd'} of $count->{'rows'} MXD");
    foreach my $id (keys (%{$records})) {
        $count->{'missing'}++;
    }
    if ($count->{'missing'}) {
        $self->{'oai'}->log ('w',  "$count->{'missing'} missing MXD");
    }
    return (1);
}


sub exists
{
    my ($self, $id) = @_;

    if (!exists ($self->{'cache'}{$id})) {
        my $rs = $self->{'db'}->select ('*', 'mxd', "id='$id'");
        my $rec;
        if ($rec = $self->{'db'}->next ($rs)) {
            $self->{'cache'}{$id} = $rec;
        } else {
            $self->{'cache'}{$id} = {};
        }
        push (@{$self->{'cacheids'}}, $id);
        if ($#{$self->{'cacheids'}} > 99) {
            my $i = shift (@{$self->{'cacheids'}});
            delete ($self->{'cache'}{$i});
        }
    }
    if (exists ($self->{'cache'}{$id}{'id'})) {
        return (1);
    } else {
        return (0);
    }
}

sub year
{
    my ($self, $id) = @_;

    return ($self->field ($id, 'year'));
}

sub type
{
    my ($self, $id) = @_;

    return ($self->field ($id, 'doc_type'));
}

sub review
{
    my ($self, $id) = @_;

    return ($self->field ($id, 'doc_review'));
}

sub level
{
    my ($self, $id) = @_;

    return ($self->field ($id, 'doc_level'));
}

sub issn
{
    my ($self, $id) = @_;

    my $s;
    my @ret = ();
    if ($s = $self->field ($id, 'issn')) {
        push (@ret, split (',', $s));
    }
    if ($s = $self->field ($id, 'eissn')) {
        push (@ret, split (',', $s));
    }
    return (@ret);
}

sub pissn
{
    my ($self, $id) = @_;

    my $s;
    my @ret = ();
    if ($s = $self->field ($id, 'issn')) {
        push (@ret, split (',', $s));
    }
    return (@ret);
}

sub eissn
{
    my ($self, $id) = @_;

    my $s;
    my @ret = ();
    if ($s = $self->field ($id, 'eissn')) {
        push (@ret, split (',', $s));
    }
    return (@ret);
}

sub xml
{
    my ($self, $id) = @_;

    return ($self->field ($id, 'original_xml'));
}

sub field
{
    my ($self, $id, $fld) = @_;

    if (exists ($self->{'cache'}{$id}{$fld})) {
        return ($self->{'cache'}{$id}{$fld});
    } else {
        return ('');
    }
}

sub fulltext
{
    my ($self, $id) = @_;

    my $rs = $self->{'db'}->select ('*', 'mxdft', "dads_id='$id'");
    my $rc;
    my @ft = ();
    while ($rc = $self->{'db'}->next ($rs)) {
        push (@ft, $rc);
    }
    return (@ft);
}

sub fulltext_exists
{
    my ($self, $id) = @_;

    if (!exists ($self->{'fulltext-cache'}{$id})) {
        my $rs = $self->{'db'}->select ('*', 'mxdft', "dads_id='$id'");
        my $rec;
        my $main = 0;
        my $files = {};
        while ($rec = $self->{'db'}->next ($rs)) {
            $files->{$rec->{'id'}} = $rec;
            if ((!$main) ||
                (($rec->{'mime'} eq 'application/pdf') && ($files->{$main}{'mime'} ne 'application/pdf')) ||
                (($rec->{'mime'} eq 'application/pdf') && ($rec->{'size'} > $files->{$main}{'size'}))) {
                $main = $rec->{'id'};
            }

        }
        if ($main) {
            $self->{'fulltext-cache'}{$id} = $files->{$main};
        } else {
            $self->{'fulltext-cache'}{$id} = {};
        }
        push (@{$self->{'fulltext-cacheids'}}, $id);
        if ($#{$self->{'fulltext-cacheids'}} > 99) {
            my $i = shift (@{$self->{'fulltext-cacheids'}});
            delete ($self->{'fulltext-cache'}{$i});
        }
    }
    if (exists ($self->{'fulltext-cache'}{$id}{'id'})) {
        return (1);
    } else {
        return (0);
    }
}

sub fulltext_type
{
    my ($self, $id) = @_;

    return ($self->fulltext_field ($id, 'type'));
}

sub fulltext_access
{
    my ($self, $id) = @_;

    return ($self->fulltext_field ($id, 'access'));
}

sub fulltext_uri
{
    my ($self, $id) = @_;

    return ($self->fulltext_field ($id, 'uri'));
}

sub fulltext_size
{
    my ($self, $id) = @_;

    return ($self->fulltext_field ($id, 'size'));
}

sub fulltext_mime
{
    my ($self, $id) = @_;

    return ($self->fulltext_field ($id, 'mime'));
}

sub fulltext_filename
{
    my ($self, $id) = @_;

    return ($self->fulltext_field ($id, 'filename'));
}

sub fulltext_field
{
    my ($self, $id, $fld) = @_;

    if (exists ($self->{'fulltext-cache'}{$id}{$fld})) {
        return ($self->{'fulltext-cache'}{$id}{$fld});
    } else {
        return ('');
    }
}

1;

