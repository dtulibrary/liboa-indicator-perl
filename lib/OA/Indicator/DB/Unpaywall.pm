package OA::Indicator::DB::Unpaywall;

use strict;
use warnings;
use OA::Indicator::DB::DOAR;
use DB::SQLite;
use LWP::UserAgent;
use HTTP::Request;
use JSON::XS;

our $VERSION = '1.0';

sub new
{
    my ($class, $db, $oai) = @_;
    my $self = {cachetime => (3600 * 24 * 60)};

    $self->{'oai'} = $oai;
    $self->{'db'} = new DB::SQLite ("/var/lib/oa-indicator/db/unpaywall.sqlite3", DieError => 1);
    $self->{'wl'} = new OA::Indicator::DB::DOAR ($db, $oai);
    $self->{'count'} = {new => 0, updated => 0};
    return (bless ($self, $class));
}

sub create
{   
    my ($self) = @_;

    my $db = $self->{'db'};
    $db->sql ('create table if not exists unpaywall (
                   id                integer primary key,
                   doi               text,
                   harvest           integer,
                   status_code       integer,
                   status_message    text,
                   json              text)');
    $db->sql ('create table if not exists upw (
                   id                integer primary key,
                   doi               text,
                   msg               text,
                   is_oa             integer,
                   is_publisher      integer,
                   is_doaj           integer,
                   is_cc_by          integer,
                   is_best           integer,
                   is_published      integer,
                   is_white          integer,
                   url               text)');
    $db->sql ('create index if not exists upw_doi on upw (doi)');
    my $rs = $db->select ('id,doi,harvest', 'unpaywall');
    my $rc;
    while ($rc = $db->next ($rs)) {
        $self->{'doi'}{$rc->{'doi'}} = $rc;
    }
}

sub doi_clean
{
    my ($self, $doi) = @_;

    $doi =~ s/^doi.org\///i;
    $doi =~ s/^DOI:?\s*//i;
    $doi =~ s/https?:\/\/doi.org\///i;
    $doi =~ s/\s+//g;
    $doi =~ s/^\///;
    $doi =~ s/[\|\,\.]+$//;
    return ($doi);
}

sub request
{
    my ($self, $doi) = @_;

    if (exists ($self->{'doi'}{$doi})) {
        if (!$self->{'doi'}{$doi}{'update'}) {
            if ((time - $self->{'doi'}{$doi}{'harvest'}) > $self->{'cachetime'}) {
                $self->{'doi'}{$doi}{'update'} = 1;
                $self->{'count'}{'updated'}++;
            }
        }
    } else {
        $self->{'doi'}{$doi} = {doi => $doi, harvest => 0, status_code => 0, status_message => '', json => '', update => 1};
        $self->{'count'}{'new'}++;
    }
    return (1);
}

sub harvest
{
    my ($self, $type) = @_;

    if (($type) && ($type =~ m/expand/i)) {
        $self->expand_all ();
        return (1);
    }
    my $total = $self->{'count'}{'new'} + $self->{'count'}{'updated'};
    $self->{'oai'}->log ('i', "%d doi to harvest, %d new, %d updated", $total, $self->{'count'}{'new'}, $self->{'count'}{'updated'});
    if (!exists ($self->{'ua'})) {
        $self->{'ua'} = new LWP::UserAgent;
        $self->{'ua'}->agent ('OA-Indicator/1.0');
        $self->{'ua'}->timeout (180);
        $self->{'ua'}->ssl_opts (SSL_verify_mode => 'SSL_VERIFY_NONE', verify_hostnames => 0);
    }
    my $done = 0;
    my $loop = 0;
    while (1) {
        foreach my $doi (sort (keys (%{$self->{'doi'}}))) {
            if ($self->{'doi'}{$doi}{'update'}) {
                my $url = 'https://api.unpaywall.org/v2/' . $self->doi_clean ($doi) . '?email=franck@cvt.dk';
                my $rs = $self->http_get ($url);
                if ($rs->is_success) {
                    my $s = $rs->header ('Client-Aborted');
                    if ((defined ($s)) && ($s !~ m/^\s*$/)) {
                        if ($s eq 'die') {
                            $self->{'oai'}->log ('e', "Client-Aborted - die - harvesting '%s'", $url);
                        } else {
                            $self->{'oai'}->log ('e', "Client-Aborted unknown - %s - harvesting '%s'", $s, $url);
                        }
                        next;
                    }
                    $s = $rs->header ('X-Died');
                    if ((defined ($s)) && ($s !~ m/^\s*$/)) {
                        $s =~ s/ at \/.*//;
                        if ($s =~ m/eof when chunk header expected/i) {
                            $self->{'oai'}->log ('e', "connection lost while harvesting '%s'", $url);
                        } elsif ($s =~ m/read timeout/i) {
                            $self->{'oai'}->log ('e', "connection timeout while harvesting '%s'", $url);
                        } else {
                            $self->{'oai'}->log ('e', "unknown X-Died header timeout while harvesting '%s': %s", $url, $s);
                        }
                        next;
                    }
                    my $content = $rs->content;
                    my $size = $rs->header ('Content-Length');
                    if (($size) && ($size > length ($content))) {
                        $self->{'oai'}->log ('e', "error for '%s': got %d bytes instead of the %d in Content-Length", $url, length ($content), $size);
                    } else {
                        $self->store ($doi, 200, '', $content);
                        $done++;
                    }
                } else {
                    if ($rs->code == 404) {
                        my $re = decode_json ($rs->content);
                        if ($re->{'message'} =~ m/invalid doi/) {
                            $self->{'oai'}->log ('e', "invalid DOI: %s - %s", $doi, $re->{'message'});
                            $self->store ($doi, 404, 'invalid doi', '');
                            $done++;
                        } else {
                            $self->{'oai'}->log ('e', "harvest error: %s - %s - %s", $rs->code, $rs->message, $re->{'message'});
                        }
                    } elsif ($rs->code == 422) {
                        my $re = decode_json ($rs->content);
                        $self->{'oai'}->log ('e', "harvest error: %s - %s - %s", $rs->code, $rs->message, $re->{'message'});
                        $self->store ($doi, 422, $re->message, '');
                        $done++;
                    } else {
                        $self->{'oai'}->log ('e', "harvest error: %s - %s", $rs->code, $rs->message);
                    }
                }
                $self->{'oai'}->log ('i', "harvested %d of %d: %s", $done, $total, $url);
            }
        }
        if ($done == $total) {
            last;
        } else {
            $loop++;
            if ($loop > 6) {
                $self->{'oai'}->log ('f', "too many tries trying to harvest unpaywall");
                $self->{'oai'}->log ('f', 'fail');
                return (0);
            } else {
                $self->{'oai'}->log ('i', 'doing loop %d to harvest %d DOI still pending', $loop, ($total - $done));
            }
        }
    }
    $self->{'oai'}->log ('i', 'done');
    return (1);
}

sub store
{
    my ($self, $doi, $code, $msg, $json) = @_;

    my $db = $self->{'db'};
    if ($self->{'doi'}{$doi}{'id'}) {
        my $rc = {id => $self->{'doi'}{$doi}{'id'}, harvest => time, status_code => $code, status_message => $msg, json => $json};
        $db->update ('unpaywall', 'id', $rc);
        $self->{'doi'}{$doi}{'update'} = 0;
    } else {
        my $rc = {doi => $doi, harvest => time, status_code => $code, status_message => $msg, json => $json};
        $db->insert ('unpaywall', $rc);
        $self->{'doi'}{$doi}{'update'} = 0;
    }
    $self->expand ($doi, $msg, $json);
}

sub expand_all
{
    my ($self) = @_;

    my $db = $self->{'db'};
    my $rs = $db->select ('doi,status_message,json', 'unpaywall');
    my $rc;
    while ($rc = $db->next ($rs)) {
        $self->expand ($rc->{'doi'}, $rc->{'status_message'}, $rc->{'json'});
    }
}

sub expand
{
    my ($self, $doi, $msg, $json) = @_;
    
    my $db = $self->{'db'};
    $db->sql ("delete from upw where doi='$doi'");
    if ($msg) {
        $msg =~ s/\s+/-/g;
        my $rc = {doi => $doi, msg => $msg, is_oa => 0, is_publisher => 0, is_doaj => 0, is_cc_by => 0, is_best => 0, is_published => 0, is_white => 0, url => ''};
        $db->insert ('upw', $rc);
        return (1);
    }
    my $re = decode_json ($json);
    if (!$re->{'is_oa'}) {
        my $rc = {doi => $doi, msg => '', is_oa => 0, is_publisher => 0, is_doaj => 0, is_cc_by => 0, is_best => 0, is_published => 0, is_white => 0, url => ''};
        $rc->{'msg'} = 'not-oa';
        $db->insert ('upw', $rc);
        return (1);
    }
    my $rc = {doi => $doi, msg => '', is_oa => 1, is_publisher => 0, is_doaj => 0, is_cc_by => 0, is_best => 0, is_published => 0, is_white => 0, url => ''};
    if ($re->{'best_oa_location'}) {
        $rc->{'is_best'} = 1;
        if ($re->{'best_oa_location'}{'host_type'} eq 'publisher') {
            $rc->{'is_publisher'} = 1;
            if (!$re->{'journal_is_in_doaj'}) {
                if (($re->{'best_oa_location'}{'license'}) &&
                    ($re->{'best_oa_location'}{'license'} =~ m/^(cc-by|cc-by-nc|cc-by-nc-nd|cc-by-nc-sa|cc-by-nd|cc-by-sa)$/)) {
                    $rc->{'is_cc_by'} = 1;
                    if ($re->{'best_oa_location'}{'version'} eq 'publishedVersion') {
                        $rc->{'is_published'} = 1;
                        $rc->{'url'} = $re->{'best_oa_location'}{'url_for_landing_page'};
                        $db->insert ('upw', $rc);
                    } else {
                        $rc->{'msg'} = 'not-published';
                        $db->insert ('upw', $rc);
                    }
                } else {
                    $rc->{'msg'} = 'not-cc-by';
                    $db->insert ('upw', $rc);
                }
            } else {
                $rc->{'is_doaj'} = 1;
                $rc->{'msg'} = 'in-doaj';
                $db->insert ('upw', $rc);
            }
        } else {
            $rc->{'msg'} = 'not-publisher';
            $db->insert ('upw', $rc);
        }
    } else {
        $rc->{'msg'} = 'not-best';
        $db->insert ('upw', $rc);
    }
    $rc = {doi => $doi, msg => '', is_oa => 1, is_publisher => 0, is_doaj => 0, is_cc_by => 0, is_best => 0, is_published => 0, is_white => 0, url => ''};
    if ($re->{'oa_locations'}) {
        my $white = 0;
        foreach my $loc (@{$re->{'oa_locations'}}) {
            if ($self->{'wl'}->valid ($loc->{'url'})) {
                $white++;
                $rc->{'is_white'} = 1;
                $rc->{'url'} = $loc->{'url'};
                $db->insert ('upw', $rc);
            }
        }
        if (!$white) {
            $rc->{'msg'} = 'not-white';
            $db->insert ('upw', $rc);
        }
    } else {
        $rc->{'msg'} = 'not-locations';
        $db->insert ('upw', $rc);
    }
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

sub doi_list
{
    my ($self, $doi) = @_;

    if ($doi) {
        return ($self->{'doi_list'}{$doi});
    } else {
        my $db = $self->{'db'};
        my $rs = $db->select ('doi,msg,url,is_best', 'upw', '', 'order by is_best');
        my $rc;
        my $done = {};
        while ($rc = $db->next ($rs)) {
            if ($rc->{'msg'}) {
                if ($self->{'doi_list'}{$rc->{'doi'}}{'msg'}) {
                    $self->{'doi_list'}{$rc->{'doi'}}{'msg'} .= '; ' . $rc->{'msg'};
                } else {
                    $self->{'doi_list'}{$rc->{'doi'}}{'msg'} = $rc->{'msg'};
                }
            } else {
                if ($rc->{'is_best'}) {
                    $self->{'doi_list'}{$rc->{'doi'}}{'pub'} = $rc->{'url'};
                } else {
                    my ($domain) = $self->{'wl'}->extract_domain_path ($rc->{'url'});
                    if (!exists ($done->{$rc->{'doi'}}{$domain})) {
                        if ($self->{'doi_list'}{$rc->{'doi'}}{'rep'}) {
                            $self->{'doi_list'}{$rc->{'doi'}}{'rep'} .= '; ' . $rc->{'url'};
                        } else {
                            $self->{'doi_list'}{$rc->{'doi'}}{'rep'} = $rc->{'url'};
                        }
                        $done->{$rc->{'doi'}}{$domain} = 1;
                    }
                }
            }
        }
        return (sort (keys (%{$self->{'doi_list'}})));
    }
}

1;

