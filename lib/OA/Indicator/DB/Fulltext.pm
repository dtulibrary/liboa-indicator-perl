package OA::Indicator::DB::Fulltext;

use strict;
use warnings;
use DB::SQLite;
use LWP::UserAgent;
use HTTP::Request;
use Digest::MD5 qw(md5_hex);

our $VERSION = '1.0';

sub new
{
    my ($class, $oai) = @_;
    my $self = {cachetime => (3600 * 24 * 31)};
    $self->{'cachetime'} = (3600 * 24 * 100);

    $self->{'oai'} = $oai;
    $self->{'db'} = new DB::SQLite ("/var/lib/oa-indicator/db/fulltext.sqlite3", DieError => 1);
    return (bless ($self, $class));
}

sub create
{   
    my ($self) = @_;

    my $db = $self->{'db'};
    $db->sql ('create table if not exists fulltext_requests (
                   id                    integer primary key,
                   dsid                  text,
                   type                  text,
                   url                   text,
                   requested_first       integer,
                   requested_last        integer,
                   size                  integer,
                   mime                  text,
                   filename              text,
                   pending               integer,
                   status                text
              )');
    $db->sql ('create index if not exists fulltext_requests_dsid on fulltext_requests (dsid)');
    $db->sql ('create index if not exists fulltext_requests_url on fulltext_requests (url)');
    $db->sql ('create index if not exists fulltext_requests_pending on fulltext_requests (pending)');
    $db->sql ('create table if not exists fulltext (
                   url                   text primary key,
                   harvested_first       integer,
                   harvested_last        integer,
                   http_code             text,
                   http_message          text,
                   success               integer,
                   errors_consecutive    integer,
                   errors_total          integer,
                   error_message         text,
                   size                  integer,
                   mime                  text,
                   filename              text,
                   md5                   text,
                   pdf_created           text,
                   pdf_updated           text,
                   pdf_pages             integer
              )');
}

sub request
{
    my ($self, $id, $type, $url, $size, $mime, $filename) = @_;

    if ($url =~ m/^[\s\t\r\n]*$/) {
        $self->{'oai'}->log ('w', "skipping request with empty URL: %s", $id);
        return (0);
    }
    if ($url !~ m/^https?:/i) {
        $self->{'oai'}->log ('w', "skipping request with unsupported URL protocol: %s : %s", $id, $url);
        return (0);
    }
    my $db = $self->{'db'};
    my $rs = $db->select ('*', 'fulltext_requests', "dsid='$id' and url='$url'");
    my $rec;
    if ($rec = $db->next ($rs)) {
        $rec->{'type'} = $type;
        $rec->{'size'} = $size;
        $rec->{'mime'} = $mime;
        $rec->{'pending'} = '1';
        $rec->{'filename'} = $filename;
        $rec->{'requested_last'} = time;
        $rec->{'status'} = '';
        $db->update ('fulltext_requests', 'id', $rec);
    } else {
        $rec = {dsid => $id, type => $type, url => $url, requested_first => time, requested_last => time,
                size => $size, mime => $mime, filename => $filename, pending => 1};
        $db->insert ('fulltext_requests', $rec);
    }
    return (1);
}

sub harvest
{
    my ($self) = @_;

    my $db = $self->{'db'};
    my $rs = $db->select ('*', 'fulltext_requests', 'pending=1');
    my @queue = ();
    my $count = {cache => 0, done => 0, hosts => 0, url => 0};
    my $rec;
    while ($rec = $db->next ($rs)) {
        push (@queue, $rec);
        $count->{'url'}++;
    }
    my $queue = {};
    foreach $rec (@queue) {
        $rs = $db->select ('*', 'fulltext', "url='$rec->{'url'}'");
        my $rc;
        if ($rc = $db->next ($rs)) {
            if (($rc->{'success'}) && (($rc->{'harvested_last'} + $self->{'cachetime'}) >= time)) {
                $self->{'oai'}->log ('i', "skipping harvest of cached URL: %s", $rec->{'url'});
                $count->{'cache'}++;
                $rec->{'pending'} = 0;
                $rec->{'status'} = 'ok';
                $db->update ('fulltext_requests', 'id', $rec);
                next;
            }
        } else {
            $rc = {new => 1, url => $rec->{'url'}, harvested_first => time, errors_consecutive => 0, errors_total => 0};
        }
        $count->{'harvest'}++;
        my $host = $self->host ($rec->{'url'});
        if (exists ($queue->{$host})) {
            push (@{$queue->{$host}}, [$rec, $rc]);
        } else {
            $count->{'hosts'}++;
            $queue->{$host} = [[$rec, $rc]];
        }
    }
    $self->{'oai'}->log ('i', "%d URL, %d URL cached, %d URL to harvest from %d hosts", $count->{'url'}, $count->{'cache'}, $count->{'harvest'}, $count->{'hosts'});
    my $match = 1;
    my $done = {};
    while ($match) {
        $match = 0;
        foreach my $host (keys (%{$queue})) {
            $match = 1;
            my $entry = shift (@{$queue->{$host}});
            if ($#{$queue->{$host}} == -1) {
                 $self->{'oai'}->log ('i', "done harvesting host: %s", $host);
                delete ($queue->{$host});
            }
            my ($rec, $rc) = @{$entry};
            if ($done->{$rec->{'url'}}) {
                $self->{'oai'}->log ('i', "processing already done URL: %s", $rec->{'url'});
                if ($done->{$rec->{'url'}}{'status'} eq 'ok') {
                    $rec->{'pending'} = 0;
                    $rec->{'status'} = 'ok';
                } else {
                    $rec->{'status'} = 'error';
                }
                $db->update ('fulltext_requests', 'id', $rec);
                $count->{'done'}++;
                next;
            } else {
                $self->{'oai'}->log ('i', "processing new URL: %s", $rec->{'url'});
            }
            if ($self->file_harvest ($rec, $rc)) {
                $rec->{'pending'} = 0;
                $rec->{'status'} = 'ok';
                $db->update ('fulltext_requests', 'id', $rec);
            } else {
                $rec->{'status'} = 'error';
                $db->update ('fulltext_requests', 'id', $rec);
            }
            if ($rc->{'new'}) {
                delete ($rc->{'new'});
                $db->insert ('fulltext', $rc);
            } else {
                $db->update ('fulltext', 'url', $rc);
            }
            $done->{$rec->{'url'}} = $rec;
            $count->{'done'}++;
            $self->{'oai'}->log ('i', "done %d of %d URL: %s - %s", $count->{'done'}, $count->{'harvest'}, $rc->{'http_code'}, $rec->{'url'});
        }
    }
}

sub file_harvest
{
    my ($self, $req, $rec, $redirect) = @_;

    if (!exists ($self->{'ua'})) {
        $self->{'ua'} = new LWP::UserAgent;
        $self->{'ua'}->agent ('OA-Indicator/1.0');
        $self->{'ua'}->timeout (180);
        $self->{'ua'}->ssl_opts (SSL_verify_mode => 'SSL_VERIFY_NONE', verify_hostnames => 0);
    }
#   my $re = new HTTP::Request ('GET' => $req->{'url'});
#   my $rs = $self->{'ua'}->request ($re);
    my $rs = $self->http_get ($req->{'url'});
    $rec->{'http_code'} = $rs->code;
    $rec->{'http_message'} = $rs->message;
    if ($rec->{'success'} = $rs->is_success) {
        $rec->{'success'} = 1;
        my $s = $rs->header ('Client-Aborted');
        if ((defined ($s)) && ($s !~ m/^\s*$/)) {
            $rec->{'client_aborted'} = $s;
            if ($s eq 'die') {
                $rec->{'success'} = 0;
                $rec->{'error_message'} = 'client_aborted - die';
            } else {
                $rec->{'success'} = 0;
                $rec->{'error_message'} = 'client_aborted - unknown';
                $self->{'oai'}->log ('e', "unknown Client-Aborted header while harvesting '%s': %s", $req->{'url'}, $s);
            }
        }
        $s = $rs->header ('X-Died');
        if ((defined ($s)) && ($s !~ m/^\s*$/)) {
            $s =~ s/ at \/.*//;
            $rec->{'x_died'} = $s;
            if ($s =~ m/eof when chunk header expected/i) {
                $rec->{'success'} = 0;
                $rec->{'error_message'} = 'connection lost';
            } elsif ($s =~ m/read timeout/i) {
                $rec->{'success'} = 0;
                $rec->{'error_message'} = 'connection timeout';
            } else {
                $rec->{'success'} = 0;
                $rec->{'error_message'} = 'unknown error';
                $self->{'oai'}->log ('e', "unknown X-Died header while harvesting '%s': %s", $req->{'url'}, $s);
            }
        }
    } else {
        $rec->{'success'} = 0;
        $rec->{'error_message'} = $rec->{'http_code'} . ' : ' . $rec->{'http_message'};
    }
    my $content;
    if ($rec->{'success'}) {
        $content = $rs->content;
        $rec->{'size'} = $rs->header ('Content-Length');
        if ($rec->{'size'}) {
            if ($rec->{'size'} > length ($content)) {
                $rec->{'success'} = 0;
                $rec->{'error_message'} = 'short file - based on Content-Length';
                $self->{'oai'}->log ('e', "error for '%s': got %d bytes instead of the %d in Content-Length", $req->{'url'}, length ($content), $rec->{'size'});
            }
        } else {
            $rec->{'size'} = length ($content);
        }
    }
    my $tmpfile;
    if ($rec->{'success'}) {
        $rec->{'mime'} = $rs->header ('Content-Type');
        $rec->{'filename'} = $rs->filename;
        $rec->{'filename'} =~ s/\.pdf\.pdf$/.pdf/i;
        if (!$rec->{'mime'}) {
            if ($req->{'mime'}) {
                $rec->{'mime'} = $req->{'mime'};
            } else {
                if ($rec->{'filename'} =~ m/\.pdf$/i) {
                    $rec->{'mime'} = 'application/pdf';
                }
            }
        }
        $rec->{'md5'} = md5_hex ($content);
        $tmpfile = '/var/lib/oa-indicator/tmp/' . $rec->{'md5'} . '.ft';
        if (open (my $fou, "> $tmpfile")) {
            print ($fou $content);
            close ($fou);
        } else {
            $self->{'oai'}->log ('f', "error creating file '%s': %s", $tmpfile, $!);
            exit (1);
        }
        if ($rec->{'mime'} eq 'application/pdf') {
            open (my $fin, "/usr/bin/pdfinfo $tmpfile |");
            my @lines = ();
            while (<$fin>) {
                push (@lines, $_);
                chomp;
                if (m/CreationDate:\s*(.*)/) {
                    $rec->{'pdf_created'} = $1;
                }
                if (m/ModDate:\s*(.*)/) {
                    $rec->{'pdf_updated'} = $1;
                }
                if (m/Pages:\s*(.*)/) {
                    $rec->{'pdf_pages'} = $1;
                }
            }
            close ($fin);
            if (!$rec->{'pdf_pages'}) {
                $rec->{'success'} = 0;
                $rec->{'error_message'} = 'PDF error';
                $self->{'oai'}->log ('e', "PDF error for '%s':\n%s", $req->{'url'}, join ('', @lines));
            }
        }
    }
    if ($rec->{'success'}) {
        if (($req->{'size'}) && ($req->{'size'} > $rec->{'size'})) {
            if ($rec->{'pdf_pages'}) {
                $self->{'oai'}->log ('w', "error for '%s': got %d bytes instead of the %d in request, but PDF is valid",
                                     $req->{'url'}, $rec->{'size'}, $req->{'size'});
            } else {
                $rec->{'success'} = 0;
                $rec->{'error_message'} = 'short file -  based on request size';
                $self->{'oai'}->log ('e', "error for '%s': got %d bytes instead of the %d in request", $req->{'url'}, $rec->{'size'}, $req->{'size'});
            }
        }
    }
    if ($rec->{'success'}) {
        $rec->{'errors_consecutive'} = 0;
        $rec->{'harvested_last'} = time;
        $rec->{'error_message'} = '';
        my $dir = '/var/lib/oa-indicator/ft/' . substr ($rec->{'md5'}, 0, 2);
        if (!-e $dir) {
            if (!mkdir ($dir, 0775)) {
                $self->{'oai'}->log ('f', "error creating directory '%s': %s", $dir, $!);
                exit (1);
            }
        }
        if (!rename ($tmpfile, "$dir/$rec->{'md5'}.dat")) {
            $self->{'oai'}->log ('f', "rename error: '%s' -> '%s/%s.dat' (%s)", $tmpfile, $dir, $rec->{'md5'}, $!);
            exit (1);
        }
    } else {
        $rec->{'errors_consecutive'}++;
        $rec->{'errors_total'}++;
    }
    return ($rec->{'success'});
}

sub http_get
{
    my ($self, $url, $redirect) = @_;

    if (!defined ($redirect)) {
        $redirect = 0;
    }
    my $re = new HTTP::Request ('GET' => $url);
    my $rs = $self->{'ua'}->request ($re);
    my $code = $rs->code;
    if (($code == 301) || ($code == 302)) {
        my $location = $rs->header ('Location');
        if ($location) {
            if ($redirect < 6) {
                if ($location eq $url) {
                    $self->{'oai'}->log ('i', "ignore redirect to same location: '%s'", $location);
                } else {
                    $self->{'oai'}->log ('i', "redirect from '%s' to '%s'", $url, $location);
                    return ($self->http_get ($location, $redirect + 1));
                }
            } else {
                $self->{'oai'}->log ('i', "exceeded number of redirect: $redirect");
            }
        }
    }
    return ($rs);
}

sub host
{
    my ($self, $url) = @_;

    if ($url =~ m|//([^/\?\:]+)|) {
        return ($1);
    } else {
        $self->{'oai'}->log ('w', "could not extract hostname from: %s", $url);
        return ('undefined');
    }
}

1;

