package OA::Indicator::QA::Extracts;

use strict;
use warnings;
use Fatal qw(open);
use DB::SQLite;
use OA::Indicator;
use OA::Indicator::XML;
use OA::Indicator::DB;
use OA::Indicator::DB::DOAR;

sub new
{
    my ($class, @years) = @_;

    my $self = {};
    my $db = new OA::Indicator::DB ();
    foreach my $year (@years) {
        my $file = $db->run_db ($year, 'prod');
        if ((!$file) || (!-e $file)) {
            die ("fatal: could not find database file for $year");
        }
        warn ("$year DB file: $file\n");
        $self->{"db.$year"} = new DB::SQLite ($file);
        $self->{'db'} = $self->{"db.$year"};
    }
    $self->{'ft'}   = new DB::SQLite ('/var/lib/oa-indicator/db/fulltext.sqlite3');
    my $oai         = new OA::Indicator (verbose => 1);
    $self->{'wl'}   = new OA::Indicator::DB::DOAR ($self->{'db'}, $oai);
    $self->{'year'} = pop (@years);
    return (bless ($self, $class));
}

sub ids
{
    my ($self, $year, $id) = @_;

    if (!exists ($self->{"ids.$year"})) {
        my $rs = $self->{"db.$year"}->select ('id,source_id', 'records', 'fulltext_link_oa=1');
        my $rc;
        while ($rc = $self->{"db.$year"}->next ($rs)) {
            $self->{"ids.$year"}{$rc->{'id'}} = $rc->{'source_id'};
        }
    }
    return ($self->{"ids.$year"}{$id});
}

sub records
{
    my ($self, $id, $fld) = @_;

    if (!exists ($self->{'records'})) {
        my $rs = $self->{'db'}->select ('id,source,source_id,dedupkey,title', 'records', 'fulltext_link_oa=1');
        my $rc;
        while ($rc = $self->{'db'}->next ($rs)) {
            foreach my $f (qw(source source_id dedupkey title)) {
                $self->{'records'}{$rc->{'id'}}{$f} = $rc->{$f};
            }
        }
    }
    if ($fld !~ m/^(source|source_id|dedupkey|title)$/) {
        die ("Extracts::records invalid field: '$fld'");
    }
    if ($self->{'records'}{$id}) {
        return ($self->{'records'}{$id}{$fld});
    } else {
        return (undef);
    }
}

sub pages_extract
{
    my ($self) = @_;

    my $cfile = '/var/lib/oa-indicator/runs/' . $self->{'year'} . '.prod/cache';
    if (!-d $cfile) {
        warn ("error: directory not found: '$cfile'\n");
        die ("fatal: QA only available for production runs after spreadsheets run.\n");
    }
    $cfile .= '/qa-pages-extract.tab';
    if (-e $cfile) {
        warn ("using cache file: $cfile\n");
        open (my $fin, $cfile);
        while (<$fin>) {
            chomp;
            my ($sid, $pages) = split ("\t");
            $self->{'pages_extract'}{$sid} = $pages;
        }
        close ($fin);
    } else {
        warn ("extracting pages information...\n");
        my $xml = new OA::Indicator::XML (mxd => 'mxdns');
        foreach my $source (qw(aau au cbs dtu itu ku ruc sdu)) {
            warn ("    loading $source...\n");
            open (my $fin, "/var/lib/oa-indicator/$self->{'year'}/mxd/$source.xml");
            my $buf = '';
            my $count = {start => time};
            while (<$fin>) {
                $buf .= $_;
                $buf =~ s/^[\s\t\n\r]+//;
                $buf =~ s/^<\?xml [^>]*\?>//;
                $buf =~ s/^<records>//;
                while ($buf =~ s/^[\s\t\n\r]*(<mxd:ddf_doc.*?<\/mxd:ddf_doc>)//s) {
                    eval {
                        $xml->parse ($1);
                    };
                    $count->{'rec'}++;
                    my $sid = $xml->field ('/mxd:ddf_doc/@rec_id');
                    my @pages = $xml->field ('//mxd:pages');
                    if ($#pages > 0) {
                        warn ("multiple pages for $sid: " . join (', ', @pages) . "\n");
                    }
                    my $pages = shift (@pages);
                    if (defined ($pages)) {
                        $pages =~ s/[\s\t\r\n]+/ /g;
                        $pages =~ s/^\s//;
                        $pages =~ s/\s$//;
                        my $p = $pages;
                        $p =~ s/[^-0-9]//g;
                        if ($p ne $pages) {
                            warn ("strange pages: $pages\n");
                        }
                        $self->{'pages_extract'}{$sid} = $pages;
                    }
                    if (($count->{'rec'} % 1000) == 0) {
                        printf (STDERR "    loaded %d records, %0.2f rec/sec\n", $count->{'rec'}, ($count->{'rec'} / (time - $count->{'start'} + 0.0000001)));
                    }
                }
            }
            close ($fin);
            printf (STDERR "    loaded %d records, %0.2f rec/sec\n", $count->{'rec'}, ($count->{'rec'} / (time - $count->{'start'} + 0.0000001)));
        }
        open (my $fou, "> $cfile");
        foreach my $sid (sort (keys (%{$self->{'pages_extract'}}))) {
            print ($fou $sid, "\t", $self->{'pages_extract'}{$sid}, "\n");
        }
        close ($fou);
    }
}

sub pages
{
    my ($self, $sid) = @_;

    if ($self->{'pages_extract'}{$sid}) {
        my $pages = $self->{'pages_extract'}{$sid};
        if ($pages =~ m/[0-9]/) {
            if ($pages =~ m/-/) {
                my ($from, $to) = split ('-', $pages);
                if (length ($from) > length ($to)) {
                    return ($to - substr ($from, (length ($from) - length ($to))));
                } else {
                    if ($from > $to) {
                        warn ("pages error: $pages ($sid)\n");
                        return (-1);
                    } else {
                        return ($to - $from);
                    }
                }
            } else {
                return ($pages);
            }
        } else {
            return (0);
        }
    } else {
        return (0);
    }
}

sub multiple
{
    my ($self, $did) = @_;

    if (!exists ($self->{'multiple'})) {
        my $ids = {};
        my $rs = $self->{'ft'}->select ('dsid,type,url', 'fulltext_requests', "status='ok'");
        my $rc;
        while ($rc = $self->{'ft'}->next ($rs)) {
            if (($rc->{'type'} eq 'loc') || ($self->{'wl'}->valid ($rc->{'url'}))) {
                $ids->{$rc->{'dsid'}}++;
            }
        }
        foreach my $id (keys (%{$ids})) {
            if ($ids->{$id} > 1) {
                $self->{'multiple'}{$id} = 1;
            }
        }
    }
    return ($self->{'multiple'}{$did});
}

sub fulltext
{
    my ($self, $did, $fld) = @_;

    if (!exists ($self->{'fulltext'})) {
        my $rs = $self->{'ft'}->select ('dsid,type,f.mime,f.size,pdf_pages,f.url,md5,f.filename', 'fulltext_requests as r,fulltext as f', 'r.url=f.url');
        my $rc;
        my $dup = {};
        while ($rc = $self->{'ft'}->next ($rs)) {
            if (!defined ($rc->{'url'})) {
                warn ("non-defined URL for $rc->{'dsid'}: " . join (',', sort (keys (%{$rc}))));
            }
            if (($rc->{'type'} eq 'rem') && (!$self->{'wl'}->valid ($rc->{'url'}))) {
                next;
            }
            foreach my $f (keys (%{$rc})) {
                my $s = $f;
                $s =~ s/^[rf]\.//;
                if ($s ne $f) {
                    if (exists ($rc->{$s})) {
                        die ("Extract::fulltext duplicate field: $s ($f)");
                    }
                    $rc->{$s} = $rc->{$f};
                    delete ($rc->{$f});
                }
            }
            if (exists ($self->{'fulltext'}{$rc->{'dsid'}})) {
                $dup->{$rc->{'dsid'}} = 1;
                my $n = '02';
                while (exists ($self->{'fulltext'}{$rc->{'dsid'} . '-' . $n})) {
                    $n = sprintf ('%02d', $n + 1);
                }
                $self->{'fulltext'}{$rc->{'dsid'} . '-' . $n} = $rc;
            } else {
                $self->{'fulltext'}{$rc->{'dsid'}} = $rc;
            }
        }
        foreach my $id (keys (%{$dup})) {
            $self->{'fulltext'}{"$id-01"} = $self->{'fulltext'}{$id};
            delete ($self->{'fulltext'}{$id});
        }
    }
    if (defined ($did)) {
        if ($fld =~ m/^(dsid|type|mime|size|pdf_pages|url|md5|filename)$/) {
            return ($self->{'fulltext'}{$did}{$fld});
        } else {
            die ("Extract::fulltext invalid field: $fld");
        }
    } else {
        return (sort (keys (%{$self->{'fulltext'}})));
    }
}

1;

