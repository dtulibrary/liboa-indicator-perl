package OA::Indicator::DB::BFI;

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
    $db->sql ('create table if not exists mxd (
                   id              text primary key,
                   source          text,
                   source_id       text,
                   year            integer,                                                                                                                
                   doc_type        text,                                                                                                                
                   doc_review      text,                                                                                                                
                   doc_level       text,                                                                                                                
                   issn            text,
                   eissn           text,
                   original_xml    text
              )');
    $db->sql ('create index if not exists bfi_source_id on mxd (source_id)');
    $db->sql ('create table if not exists mxdft (
                   id              text primary key,
                   source_id       text,
                   type            text,
                   uri             text,
                   text            text,                                                                                                                
                   role            text,                                                                                                                
                   access          text,                                                                                                                
                   size            integer,
                   mimen           text,
                   filename        text
              )');
    $db->sql ('create index if not exists mxdft_source_id on mxdft (source_id)');
}

sub load
{
    my ($self, $year) = @_;

    foreach my $src (qw(aau au cbs dtu itu ku ruc sdu)) {
        $self->load_source ($year, $src);
    }
}

sub load_source
{
    my ($self, $year, $src) = @_;
    my ($fin);

    my $issn = {};
    if (!open ($fin, "/var/lib/oa-indicator/$year/mxd/$src.issn")) {
        die ("fatal: failed to open /var/lib/oa-indicator/$year/mxd/$src.issn ($!)");
    }
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my ($id, $type, $i) = split ("\t");
        $i = uc ($i);
        $i =~ s/[^0-9X]//g;
        $issn->{$id}{$type}{$i} = 1;
    }
    close ($fin);
    my $idmap = {};
    my $records = {};
    if (!open ($fin, "/var/lib/oa-indicator/$year/mxd/$src.ids")) {
        die ("fatal: failed to open /var/lib/oa-indicator/$year/mxd/$src.ids ($!)");
    }
    my $count = {};
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my $rec = {};
        my @fields = split ("\t");
        foreach my $fld (qw(id stamp date source_id year type level review)) {
            $rec->{$fld} = shift (@fields);
        }
        $rec->{'mods'} = $rec->{'original_xml'} = '';
        $idmap->{$rec->{'id'}} = $rec->{'source_id'};
        $records->{$rec->{'source_id'}} = $rec;
        $count->{'rows'}++;
    }
    close ($fin);
    if (!open ($fin, "/var/lib/oa-indicator/$year/mxd/$src.ftx")) {
        die ("fatal: failed to open /var/lib/oa-indicator/$year/mxd/$src.ftx ($!)");
    }
    while (<$fin>) {
        chomp;
        if (m/^#/) {
            next;
        }
        my $rec = {};
        my @fields = split ("\t");
        foreach my $fld (qw(id type uri text role access size mime filename)) {
            $rec->{$fld} = shift (@fields);
        }
        $rec->{'source_id'} = $idmap->{$rec->{'id'}};
        $self->{'db'}->insert ('mxdft', $rec);
    }
    if (!open ($fin, "/var/lib/oa-indicator/$year/mxd/$src.xml")) {
        die ("fatal: failed to open /var/lib/oa-indicator/$year/mxd/$src.xml ($!)");
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
                    die ("fatal: id does not exist: $id in $src\n");
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
                    warn ("        loaded $count->{'mxd'} of $count->{'rows'}\n");
                }
            } else {
                die ("fatal: could not get identifier from MXD XML:\n$xml\n");
            }
        }
    }
    foreach my $id (keys (%{$records})) {
        $count->{'missing'}++;
    }
    if ($count->{'missing'}) {
        warn ("$count->{'missing'} missing MXD\n");
    }
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

