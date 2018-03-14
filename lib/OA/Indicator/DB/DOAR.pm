package OA::Indicator::DB::DOAR;

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
    $db->sql ('create table if not exists doar (
                   id              text primary key,
                   harvest_stamp   integer,
                   harvest_date    text,
                   code            text,
                   name            text,
                   type            text,
                   uri             text,
                   domain          text,
                   path            text,
                   proposer        text,
                   usage_records   integer,
                   mods            text,
                   original_xml    text
              )');
    $db->sql ('create index if not exists doar_domain on doar (domain)');
    $db->sql ('create index if not exists doar_path on doar (path)');
}

sub load
{
    my ($self, $year) = @_;
    my ($fin);

    if (!open ($fin, "zcat /var/lib/oa-indicator/$year/doar/doar.ids.gz |")) {
        $self->{'oai'}->log ('f',  "failed to open /var/lib/oa-indicator/$year/doar/doar.ids.gz ($!)");
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
        for my $fld (qw(id harvest_stamp harvest_date status code name type uri proposer)) {
            $rec->{$fld} = shift (@fields);
        }
        if ($rec->{'status'} ne 'deleted') {
            ($rec->{'domain'}, $rec->{'path'}) = $self->extract_domain_path ($rec->{'uri'});
            $rec->{'usage_records'} = 0;
            $rec->{'mods'} = $rec->{'original_xml'} = '';
            delete ($rec->{'status'});
            $records->{$rec->{'id'}} = $rec;
            $count->{'rows'}++;
        }
    }
    close ($fin);
    if (!open ($fin, "zcat /var/lib/oa-indicator/$year/doar/doar.xml.gz |")) {
        $self->{'oai'}->log ('f', "failed to open /var/lib/oa-indicator/$year/doar/doar.xml.gz ($!)");
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
                    $self->{'oai'}->log ('f', "id does not exist: $id in doar");
                    $self->{'oai'}->log ('f', 'failed');
                    return (0);
                }
                $records->{$id}{'mods'} = $xml;
                $self->{'db'}->insert ('doar', $records->{$id});
                delete ($records->{$id});
                $count->{'doar'}++;
                if (($count->{'doar'} % 1000) == 0) {
                    $self->{'oai'}->log ('i', "loaded $count->{'doar'} of $count->{'rows'}");
                }
            } else {
                $self->{'oai'}->log ('f', " could not get identifier from MODS XML:\n$xml");
                $self->{'oai'}->log ('f', 'failed');
                return (0);
            }
        }
    }
    $self->{'db'}->commit ();
    $self->{'oai'}->log ('i', "loaded $count->{'doar'} of $count->{'rows'}");
    foreach my $id (keys (%{$records})) {
        $count->{'missing'}++;
    }
    if ($count->{'missing'}) {
        $self->{'oai'}->log ('w', "$count->{'missing'} missing MODS");
    }
    $self->{'oai'}->log ('i',  'done');
    return (1);
}

sub extract_domain_path
{
    my ($self, $uri) = @_;

    my $path = '';
    my $domain = lc ($uri);
    $domain =~ s/^.*\/\///;
    if ($domain =~ s/\/(.*)//) {
        $path = $1;
        if ($path =~ m/^[\s\t\r\n]*$/) {
            $path = '';
        } else {
            $path = '/' . $path;
        }
    }
    return ($domain, $path);
}

sub valid
{
    my ($self, $uri) = @_;

    my ($domain, $path) = $self->extract_domain_path ($uri);
    my $rs = $self->{'db'}->select ('code,path', 'doar', "domain='$domain'");
    my $rc;
    while ($rc = $self->{'db'}->next ($rs)) {
        if (($rc->{'path'} eq '') || ($rc->{'path'} eq substr ($path, 0, length ($rc->{'path'})))) {
           return ($rc->{'code'});
        }
    }
    return (0);
}

sub usage_records
{
    my ($self, $code) = @_;

    my $rs = $self->{'db'}->select ('id,usage_records', 'doar', "code='$code'");
    my $rc;
    my @rec = ();
    while ($rc = $self->{'db'}->next ($rs)) {
        $rc->{'usage_records'}++;
        push (@rec, $rc);
    }
    if (@rec) {
        foreach my $rc (@rec) {
            $self->{'db'}->update ('doar', 'id', $rc);
            $self->{'db'}->commit ();
        }
    } else {
        $self->{'oai'}->log ('f',  "DB::DOAR::usage_records - code not found: '$code'");
        $self->{'oai'}->log ('f',  'failed');
        exit (1);
    }
}

1;

