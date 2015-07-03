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
                   id              text primary key,
                   source          text,
                   source_id       text,
                   stamp           integer,
                   date            text,
                   year            integer,                                                                                                                
                   doc_type        text,                                                                                                                
                   doc_review      text,                                                                                                                
                   doc_level       text,                                                                                                                
                   research_area   text,
                   jno             integer,
                   pno             integer,
                   jtitle          text,
                   jtitle_alt      text,
                   issn            text,
                   eissn           text,
                   original_xml    text
              )');
    $db->sql ('create index if not exists bfi_source_id on mxd (source_id)');
    $db->sql ('create table if not exists mxdft (
                   id              integer primary key,
                   dads_id         text,
                   source          text,
                   source_id       text,
                   type            text,
                   uri             text,
                   access          text,                                                                                                                
                   text            text,                                                                                                                
                   role            text,                                                                                                                
                   size            integer,
                   mime            text,
                   filename        text
              )');
    $db->sql ('create index if not exists mxdft_dads_id on mxdft (dads_id)');
    $db->sql ('create index if not exists mxdft_source_id on mxdft (source_id)');
    $db->sql ('create table if not exists mxd_person (
                   id              integer primary key,
                   dads_id         text,
                   source          text,
                   source_id       text,
                   role            text,
                   position        integer,
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
    my $idmap = {};
    my $records = {};
    if (!open ($fin, "/var/lib/oa-indicator/$year/mxd/$src.ids")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/mxd/$src.ids ($!)");
        $self->{'oai'}->log ('f', 'failed');
        return (0);
    }
    $count = {};
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my $rec = {};
        my @fields = split ("\t");
        foreach my $fld (qw(id stamp date source_id year doc_type doc_level doc_review research_area jno pno jtitle jtitle_alt)) {
            $rec->{$fld} = shift (@fields);
        }
        $rec->{'source'} = $src;
        $rec->{'original_xml'} = '';
        $idmap->{$rec->{'id'}} = $rec->{'source_id'};
        $records->{$rec->{'source_id'}} = $rec;
        $count->{'rows'}++;
    }
    close ($fin);
    $self->{'oai'}->log ('i',  "loaded $count->{'rows'} ids");
#   Loading fulltext, first getting digital_object URI's to then clean out duplicate URI
    if (!open ($fin, "/var/lib/oa-indicator/$year/mxd/$src.ftx")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/mxd/$src.ftx ($!)");
        $self->{'oai'}->log ('f', 'failed');
        return (0);
    }
    my $douri = {};
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my $rec = {};
        my @fields = split ("\t");
        foreach my $fld (qw(dads_id type uri)) {
            $rec->{$fld} = shift (@fields);
        }
        if ($rec->{'type'} eq 'digital_object') {
            if (exists ($douri->{$rec->{'uri'}})) {
                $self->{'oai'}->log ('w', "duplicate URI between $rec->{'dads_id'} and $douri->{$rec->{'uri'}}: $rec->{'uri'}");
            } else {
                $douri->{$rec->{'uri'}} = $rec->{'dads_id'};
            }
        }
    }
    close ($fin);
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
        foreach my $fld (qw(dads_id type uri access text role size mime filename)) {
            $rec->{$fld} = shift (@fields);
        }
        $rec->{'source_id'} = $idmap->{$rec->{'dads_id'}};
        $rec->{'source'} = $src;
        if ($rec->{'type'} eq 'digital_object') {
            delete ($rec->{'text'});
            $self->{'db'}->insert ('mxdft', $rec);
            $count->{'ft'}++;
        } else {
            if (!exists ($douri->{$rec->{'uri'}})) {
                foreach my $f (qw(role size mime filename)) {
                    delete ($rec->{$f});
                }
                $self->{'db'}->insert ('mxdft', $rec);
                $count->{'ft'}++;
            }
        }
    }
    $douri = {};
    close ($fin);
    $self->{'oai'}->log ('i',  "loaded $count->{'ft'} fulltext links");
#   Person records
    if (!open ($fin, "/var/lib/oa-indicator/$year/mxd/$src.persons")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/mxd/$src.persons ($!)");
        $self->{'oai'}->log ('f', 'failed');
        return (0);
    }
    my $pos = 1;
    my $posid = '';
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my $rec = {};
        my @fields = split ("\t");
        foreach my $fld (qw(dads_id role firstname lastname email pid pid_type pid_source)) {
            $rec->{$fld} = shift (@fields);
        }
        $rec->{'source_id'} = $idmap->{$rec->{'dads_id'}};
        $rec->{'source'} = $src;
        if ($rec->{'dads_id'} eq $posid) {
            $pos++;
        } else {
            $pos = 1;
            $posid = $rec->{'dads_id'};
        }
        $rec->{'position'} = $pos;
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
                        $records->{$id}{'issn'} = join (';', sort (keys (%{$issn->{$did}{'print'}})));
                    } else {
                        $records->{$id}{'issn'} = '';
                    }
                    if ($issn->{$did}{'electronic'}) {
                        $records->{$id}{'eissn'} = join (';', sort (keys (%{$issn->{$did}{'electronic'}})));
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
        push (@ret, split ("\t", $s));
    }
    if ($s = $self->field ($id, 'eissn')) {
        push (@ret, split ("\t", $s));
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

sub fulltext_exists
{
    my ($self, $id) = @_;

    if (!exists ($self->{'fulltext-cache'}{$id})) {
        my $rs = $self->{'db'}->select ('*', 'mxdft', "dads_id='$id'");
        my $rec;
        if ($rec = $self->{'db'}->next ($rs)) {
            $self->{'fulltext-cache'}{$id} = $rec;
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

