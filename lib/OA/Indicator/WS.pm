package OA::Indicator::WS;

use strict;
use warnings;
use Time::HiRes qw(time);
use JSON::XS;
use Text::CSV;
use OA::Indicator::DB;

sub new
{
    my ($class) = @_;
    my $self = {};

    $self->{'db'} = new OA::Indicator::DB;
    return  (bless ($self, $class));
}

sub process
{
    my ($self) = @_;
    my $v;

    $self->{'start'} = time;
    if ($ENV{'QUERY_STRING'} =~ m/template=1/) {
        $self->{'template'} = 1;
    } else {
        $self->{'template'} = 0;
    }
    if ($self->{'template'}) {
        $self->{'result'} = {request => {datestamp => 'requestDatestamp'}, response => {}};
    } else {
        $self->{'result'} = {request => {datestamp => $self->date ($self->{'start'})}, response => {}};
    }
    $self->{'args'} = [];
    if (defined ($ENV{'PATH_INFO'})) {
        ($v, $self->{'comm'}, $self->{'year'}, $self->{'type'}, $self->{'run'}, @{$self->{'args'}}) = split ('/', $ENV{'PATH_INFO'});
    } else {
        $self->{'comm'} = $self->{'year'} = $self->{'type'} = $self->{'run'} = '';
    }
    if (defined ($self->{'comm'})) {
        ($self->{'comm'}, $self->{'format'}) = split (/\./, $self->{'comm'});
    } else {
        $self->{'comm'} = $self->{'format'} = '';
    }
    if ($self->{'comm'}) {
        $self->{'result'}{'request'}{'command'} = $self->{'comm'};
    } else {
        $self->{'comm'} = 'help';
        $self->{'result'}{'request'}{'command'} = 'help (default)';
    }
    if ($self->{'format'}) {
        $self->{'result'}{'request'}{'format'} = $self->{'format'};
    } else {
        if ($self->{'comm'} eq 'help') {
            $self->{'format'} = 'html';
        } else {
            $self->{'format'} = 'xml';
        }
        $self->{'result'}{'request'}{'format'} = $self->{'format'} . ' (default)';
    }
    if ($self->{'comm'} eq 'help') {
        $self->comm_help ();
    }
    if ($self->{'comm'} eq 'index') {
        $self->comm_index ();
    }
    if ($self->{'format'} !~ m/^(html|json|xml|csv)$/) {
        $self->{'format'} = 'xml';
        $self->error ('unsupported format: ' . $self->{'format'} . '. Supported formats are: json, xml and html');
    }
    if ($self->{'comm'} eq 'status') {
        $self->comm_status ();
    }
    $self->{'result'}{'request'}{'year'} = $self->{'year'};
    if ((!defined ($self->{'year'})) || ($self->{'year'} =~ m/^\s*$/)) {
        $self->error ('missing mandatory year. Year should be 4 digits.');
    }
    if ($self->{'year'} !~ m/^[0-9]{4}$/) {
        $self->error ('unsupported year: ' . $self->{'year'} . '. Year should be 4 digits.');
    }
    if ($self->{'type'}) {
        $self->{'result'}{'request'}{'type'} = $self->{'type'};
    } else {
        $self->{'type'} = 'prod';
        $self->{'result'}{'request'}{'type'} = 'prod (default)';
    }
    if ($self->{'type'} !~ m/^(devel|test|prod)$/) {
        $self->error ('unsupported type: ' . $self->{'type'} . '. Supported types are: devel, test and prod');
    }
    if ($self->{'run'}) {
        $self->{'result'}{'request'}{'run'} = $self->{'run'};
    } else {
        $self->{'run'} = '';
        $self->{'result'}{'request'}{'type'} = 'latest (default)';
    }
    if ($self->{'run'} eq 'latest') {
        $self->{'run'} = '';
    }
    my $param = $self->cgi_params ();
    if (!($self->{'types'} = $self->cgi_params ('types'))) {
        if (($self->{'type'}) && ($self->{'type'} ne 'prod')) {
            $self->{'types'} = 'prod,' . $self->{'type'};
        } else {
            $self->{'types'} = 'prod';
        }
    }
    if (!defined ($self->{'success'} = $self->cgi_params ('success'))) {
        $self->{'success'} = 1;
    }
    $self->{'result'}{'request'}{'types'} = $self->{'types'};
    $self->{'result'}{'request'}{'success'} = $self->{'success'};
    my $valid = 0;
    foreach my $v ($self->{'db'}->run_years ($self->{'types'}, $self->{'success'})) {
        if ($self->{'year'} == $v) {
            $valid = 1;
            last;
        }
    }
    if (!$valid) {
        $self->error ('no data found for year: ' . $self->{'year'});
    }
    $valid = 0;
    foreach my $v ($self->{'db'}->run_types ($self->{'year'}, $self->{'types'}, $self->{'success'})) {
        if ($self->{'type'} eq $v) {
            $valid = 1;
            last;
        }
    }
    if (!$valid) {
        $self->error ('no data found for year: ' . $self->{'year'} . ' and type: ' . $self->{'type'});
    }
    if ($self->{'run'}) {
        $valid = 0;
        foreach my $v ($self->{'db'}->run_runs ($self->{'year'}, $self->{'type'}, $self->{'success'})) {
            if ($self->{'run'} == $v) {
                $valid = 1;
                last;
            }
        }
        if (!$valid) {
            $self->error ('no data found for year: ' . $self->{'year'} . ', type: ' . $self->{'type'} . ' and run: ' . $self->{'run'});
        }
    } else {
        $self->{'run'} = 0;
        foreach my $v ($self->{'db'}->run_runs ($self->{'year'}, $self->{'type'}, $self->{'success'})) {
            if ($v > $self->{'run'}) {
                $self->{'run'} = $v;
            }
        }
    }
    if ($ENV{'QUERY_STRING'} !~ m/nocache=1/) {
        $self->cache ();
    }
    $self->{'dbkey'} = $self->{'year'} . '-' . $self->{'type'} . '-' . $self->{'run'};
    my $db;
    if (exists ($self->{'cache'}{$self->{'dbkey'}}{'db'})) {
        $db = $self->{'cache'}{$self->{'dbkey'}}{'db'};
    } else {
        $self->{'cache'}{$self->{'dbkey'}}{'db'} = $db = $self->{'db'}->reuse ($self->{'year'}, $self->{'type'}, $self->{'run'});
    }
    if ($self->{'comm'} eq 'national') {
        $self->comm_national ($db);
    }
    if ($self->{'comm'} eq 'research_area') {
        $self->comm_research_area ($db);
    }
    if ($self->{'comm'} eq 'universities') {
        $self->comm_universities ($db);
    }
    if ($self->{'comm'} eq 'records') {
        $self->comm_records ($db);
    }
    if ($self->{'comm'} eq 'publications') {
        $self->comm_publications ($db);
    }
    if ($self->{'comm'} eq 'screened') {
        $self->comm_screened ($db);
    }
    if ($self->{'comm'} eq 'extra') {
        $self->comm_extra ($db);
    }
    if ($self->{'comm'} eq 'record') {
        $self->comm_record ($db, $self->{'args'}->[0]);
    }
    if ($self->{'comm'} eq 'recordMXD') {
        $self->comm_recordMXD ($db, $self->{'args'}->[0]);
    }
    if ($self->{'comm'} eq 'recordMXDraw') {
        $self->comm_recordMXDraw ($db, $self->{'args'}->[0]);
    }
    if ($self->{'comm'} eq 'whitelist') {
        $self->comm_whitelist ($db);
    }
    if ($self->{'comm'} eq 'blacklist') {
        $self->comm_blacklist ($db);
    }
}

sub cgi_params
{
    my ($self, $key) = @_;
    if (!defined ($key)) {
        my $params = {};

        foreach my $par (split ('&', $ENV{'QUERY_STRING'})) {
            my ($name, $value) = split ('=', $par, 2);
            $name =~ s/\+/ /g;
            $name =~ s/%([0-9a-f][0-9a-f])/pack ("C", hex ($1))/gie;
            $value =~ s/\+/ /g;
            $value =~ s/%([0-9a-f][0-9a-f])/pack ("C", hex ($1))/gie;
            $params->{$name} = $value;
        }
        $self->{'cgi_params'} = $params;
        return;
    }
    if (!defined ($self->{'cgi_params'})) {
        die ('cgi_params: call $self->cgi_params (), first');
    }
    return ($self->{'cgi_params'}{$key});
}

sub comm_index
{
    my ($self) = @_;

    $self->{'result'}{'response'}{'body'}{'title'} = 'OA-Indicator';
    foreach my $year ($self->{'db'}->run_years ()) {
        foreach my $type ($self->{'db'}->run_types ($year)) {
            foreach my $run ($self->{'db'}->run_runs ($year, $type)) {
                $self->{'result'}{'response'}{'body'}{'years'}{$year}{'types'}{$type}{'runs'}{$run} = $self->{'db'}->run_info ($year, $type, $run);
            }
        }
    }
    $self->respond ();
}

sub comm_status
{
    my ($self) = @_;

    my $succ = 1;
    my $last = {};
    if ($ENV{'QUERY_STRING'} =~ m/really-all=1/) {
        $succ = $last = 0;
    } elsif ($ENV{'QUERY_STRING'} =~ m/all=1/) {
        $last = 0;
    }
    $self->{'result'}{'response'}{'body'}{'title'} = 'OA-Indicator';
    if ($last) {
        foreach my $year ($self->{'db'}->run_years ()) {
            foreach my $type ($self->{'db'}->run_types ($year)) {
                foreach my $run ($self->{'db'}->run_runs ($year, $type)) {
                    if ((!exists ($last->{$year}{$type})) || ($run > $last->{$year}{$type})) {
                        $last->{$year}{$type} = $run;
                    }
                }
            }
        }
    }
    foreach my $year ($self->{'db'}->run_years ()) {
        foreach my $type ($self->{'db'}->run_types ($year)) {
            foreach my $run ($self->{'db'}->run_runs ($year, $type)) {
                my $rec = $self->{'db'}->run_info ($year, $type, $run);
                if (($succ) && (($rec->{'success'} != 1) || ($rec->{'released'} != 1))) {
                    next;
                }
                if (($last) && ($run != $last->{$year}{$type})) {
                    next;
                }
                $self->{'result'}{'response'}{'body'}{'years'}{$year}{'types'}{$type}{'runs'}{$run} = $rec;
            }
        }
    }
    $self->respond ();
}

sub comm_help
{
    my ($self) = @_;

    $self->{'result'}{'response'}{'body'} = 'Not much help yet.';
    $self->respond ();
}

sub comm_national
{
    my ($self, $db) = @_;
    my $ret = {};

    my ($rs, $rc);
    foreach my $class (qw(realized unclear unused)) {
        $rs = $db->select ('count(distinct(dedupkey)) as c', 'records', "scoped=1 and screened=1 and pub_class='$class'");
        $rc = $db->next ($rs);
        $ret->{$class} = $rc->{'c'};
    }
    $ret->{'realized_gre_loc'} = 0;
    $ret->{'realized_gre_ext'} = 0;
    $ret->{'realized_gol_apc'} = 0;
    $ret->{'realized_gol_fre'} = 0;
    my $pub = $self->publication_realised ($db);
    foreach my $key (keys (%{$pub})) {
        foreach my $fld (qw(realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre)) {
            if ($pub->{$key}{$fld}) {
                $ret->{$fld} += $pub->{$key}{$fld};
            }
        }
    }
    $ret->{'total'} = $ret->{'realized'} + $ret->{'unclear'} + $ret->{'unused'};
    $ret->{'total_clear'} = $ret->{'realized'} + $ret->{'unused'};
    foreach my $class (qw(realized realized_gol_apc realized_gol_fre realized_gre_loc realized_gre_ext unclear unused)) {
        $ret->{'relative'}{$class} = $ret->{$class} / $ret->{'total'} * 100;
    }
    foreach my $class (qw(realized realized_gol_apc realized_gol_fre realized_gre_loc realized_gre_ext unused)) {
        $ret->{'relative_clear'}{$class} = $ret->{$class} / $ret->{'total_clear'} * 100;
    }
    $self->{'result'}{'response'}{'body'} = $ret;
    $self->respond ();
}

sub publication_realised
{
    my ($self, $db) = @_;

    my $rs = $db->select ('class_reasons,doaj_apc,fulltext_local_valid,fulltext_remote_valid,dedupkey', 'records',
                          "scoped=1 and screened=1 and pub_class='realized'");
    my $pub = {};
    my $rc;
    while ($rc = $db->next ($rs)) {
        if ($rc->{'class_reasons'} =~ m/golden/) {
            if ($rc->{'doaj_apc'}) {
                $pub->{$rc->{'dedupkey'}}{'realized_gol_apc'} = 1;
            } else {
                $pub->{$rc->{'dedupkey'}}{'realized_gol_fre'} = 1;
            }
        }
        if ($rc->{'fulltext_local_valid'}) {
            $pub->{$rc->{'dedupkey'}}{'realized_gre_loc'} = 1;
        }
        if ($rc->{'fulltext_remote_valid'}) {
            $pub->{$rc->{'dedupkey'}}{'realized_gre_ext'} = 1;
        }
    }
    return ($pub);
}

sub comm_research_area
{
    my ($self, $db) = @_;
    my $ret = {};

    my $pub = $self->publication_realised ($db);
    my ($rs, $rc);
    foreach my $area (qw(sci soc hum med)) {
        $ret->{$area}{'realized_gre_loc'} = 0;
        $ret->{$area}{'realized_gre_ext'} = 0;
        $ret->{$area}{'realized_gol_apc'} = 0;
        $ret->{$area}{'realized_gol_fre'} = 0;
        foreach my $class (qw(realized unclear unused)) {
            $rs = $db->select ('count(distinct(dedupkey)) as c', 'records', "scoped=1 and screened=1 and pub_research_area='$area' and pub_class='$class'");
            $rc = $db->next ($rs);
            $ret->{$area}{$class} = $rc->{'c'};
            if ($class eq 'realized') {
                $rs = $db->select ('distinct(dedupkey)', 'records', "scoped=1 and screened=1 and pub_research_area='$area' and pub_class='$class'");
                while ($rc = $db->next ($rs)) {
                    foreach my $fld (qw(realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre)) {
                        if ($pub->{$rc->{'dedupkey'}}{$fld}) {
                            $ret->{$area}{$fld} += $pub->{$rc->{'dedupkey'}}{$fld};
                        }
                    }
                }
            }
        }
        $ret->{$area}{'total'} = $ret->{$area}{'realized'} + $ret->{$area}{'unclear'} + $ret->{$area}{'unused'};
        $ret->{$area}{'total_clear'} = $ret->{$area}{'realized'} + $ret->{$area}{'unused'};
        foreach my $class (qw(realized realized_gol_apc realized_gol_fre realized_gre_loc realized_gre_ext unclear unused)) {
            $ret->{$area}{'relative'}{$class} = $ret->{$area}{$class} / $ret->{$area}{'total'} * 100;
        }
        foreach my $class (qw(realized realized_gol_apc realized_gol_fre realized_gre_loc realized_gre_ext unused)) {
            $ret->{$area}{'relative_clear'}{$class} = $ret->{$area}{$class} / $ret->{$area}{'total_clear'} * 100;
        }
    }
    $self->{'result'}{'response'}{'body'} = $ret;
    $self->respond ();
}

sub comm_universities
{
    my ($self, $db) = @_;
    my $ret = {};

    my $rs = $db->select ('class,source,count(*) as c', 'records', 'scoped=1 and screened=1', 'group by class,source');
    my $rc;
    while ($rc = $db->next ($rs)) {
        $ret->{$rc->{'source'}}{$rc->{'class'}} = $rc->{'c'};
        if ($rc->{'class'} eq 'realized') {
            $ret->{$rc->{'source'}}{'realized_gre_loc'} = 0;
            $ret->{$rc->{'source'}}{'realized_gre_ext'} = 0;
            $ret->{$rc->{'source'}}{'realized_gol_apc'} = 0;
            $ret->{$rc->{'source'}}{'realized_gol_fre'} = 0;
        }
    }
    $rs = $db->select ('class_reasons,doaj_apc,fulltext_local_valid,fulltext_remote_valid,source', 'records',
                       "scoped=1 and screened=1 and class='realized'");
    while ($rc = $db->next ($rs)) {
        if ($rc->{'class_reasons'} =~ m/golden/) {
            if ($rc->{'doaj_apc'}) {
                $ret->{$rc->{'source'}}{'realized_gol_apc'}++;
            } else {
                $ret->{$rc->{'source'}}{'realized_gol_fre'}++;
            }
        }
        if ($rc->{'fulltext_local_valid'}) {
            $ret->{$rc->{'source'}}{'realized_gre_loc'}++;
        }
        if ($rc->{'fulltext_remote_valid'}) {
            $ret->{$rc->{'source'}}{'realized_gre_ext'}++;
        }
    }
    foreach my $source (keys (%{$ret})) {
        $ret->{$source}{'total'} = $ret->{$source}{'realized'} + $ret->{$source}{'unclear'} + $ret->{$source}{'unused'};
        $ret->{$source}{'total_clear'} = $ret->{$source}{'realized'} + $ret->{$source}{'unused'};
        foreach my $class (qw(realized realized_gol_apc realized_gol_fre realized_gre_loc realized_gre_ext unclear unused)) {
            $ret->{$source}{'relative'}{$class} = $ret->{$source}{$class} / $ret->{$source}{'total'} * 100;
        }
        foreach my $class (qw(realized realized_gol_apc realized_gol_fre realized_gre_loc realized_gre_ext unused)) {
            $ret->{$source}{'relative_clear'}{$class} = $ret->{$source}{$class} / $ret->{$source}{'total_clear'} * 100;
        }
    }
    $self->{'result'}{'response'}{'body'} = $ret;
    $self->respond ();
}

sub comm_records
{
    my ($self, $db) = @_;

    my $duplicates = $self->duplicates ($db);
    my $rs = $db->select ('title,first_author,source,research_area,doi,issn,eissn,jtitle,jtitle_alt,series,class,class_reasons,bfi_class,bfi_level,source_id,' .
                          'dedupkey,fulltext_remote_valid,fulltext_local_valid,doaj_apc,blacklisted_issn,upw_rep,upw_pub,upw_reasons', 'records',
                          'scoped=1 and screened=1', 'order by title');
    $self->{'result'}{'response'}{'body'}{'record'} = [];
    my $rc;
    while ($rc = $db->next ($rs)) {
        if ($rc->{'issn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            $rc->{'issn'} = substr ($rc->{'issn'}, 0, 4) . '-' . uc (substr ($rc->{'issn'}, 4));
        }
        if ($rc->{'eissn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            if ($rc->{'issn'}) {
                $rc->{'issn'} .= ', ' . substr ($rc->{'eissn'}, 0, 4) . '-' . uc (substr ($rc->{'eissn'}, 4));
            } else {
                $rc->{'issn'} = substr ($rc->{'eissn'}, 0, 4) . '-' . uc (substr ($rc->{'eissn'}, 4));
            }
        }
        delete ($rc->{'eissn'});
        if (!$rc->{'jtitle'}) {
            if ($rc->{'jtitle_alt'}) {
                $rc->{'jtitle'} = $rc->{'jtitle_alt'};
            } elsif ($rc->{'series'}) {
                $rc->{'jtitle'} = $rc->{'series'};
            } else {
                $rc->{'jtitle'} = '';
            }
        }
        delete ($rc->{'jtitle_alt'});
        delete ($rc->{'series'});
        if ($rc->{'bfi_level'} == 0) {
            $rc->{'bfi_level'} = '';
        }
        $rc->{'cris_link'} = $self->cris_link ($rc->{'source'}, $rc->{'source_id'});
        if (exists ($duplicates->{$rc->{'dedupkey'}})) {
            my @id = ();
            foreach my $id (@{$duplicates->{$rc->{'dedupkey'}}{'ids'}}) {
                if ($id ne $rc->{'source_id'}) {
                    push (@id, $id);
                }
            }
            $rc->{'duplicates'} = join (', ', @id);
        } else {
            $rc->{'duplicates'} = '';
        }
        $rc->{'ddf_link'} = 'http://forskningsdatabasen.dk/en/catalog/' . $rc->{'dedupkey'};
        if ($rc->{'blacklisted_issn'}) {
            $rc->{'blacklisted_issn'} = $self->display_issn_list ($rc->{'blacklisted_issn'}, ' / ');
        }
        if ($rc->{'class'} eq 'realized') {
            if ($rc->{'class_reasons'} =~ m/golden/) {
                if ($rc->{'doaj_apc'}) {
                    $rc->{'realized_gol_apc'} = 1;
                } else {
                    $rc->{'realized_gol_fre'} = 1;
                }
            }
            if ($rc->{'fulltext_local_valid'}) {
                $rc->{'realized_gre_loc'} = 1;
            }
            if ($rc->{'fulltext_remote_valid'}) {
                $rc->{'realized_gre_ext'} = 1;
            }
        }
        push (@{$self->{'result'}{'response'}{'body'}{'record'}}, $rc);
    }
    $self->respond ();
}

sub comm_publications
{
    my ($self, $db) = @_;

    my $realised = $self->publication_realised ($db);
    my $duplicates = $self->duplicates ($db, merge => 1);
    my $rs = $db->select ('title,first_author,source,research_area,pub_research_area,bfi_research_area,doi,issn,eissn,jtitle,jtitle_alt,series,class,pub_class,pub_class_reasons,bfi_class,bfi_level,source_id,dedupkey',
                          'records', 'scoped=1 and screened=1', 'order by title');
    $self->{'result'}{'response'}{'body'}{'publication'} = [];
    my $rc;
    my $done = {};
    while ($rc = $db->next ($rs)) {
        if ($done->{$rc->{'dedupkey'}}) {
            next;
        }
        if ($duplicates->{$rc->{'dedupkey'}}) {
            $done->{$rc->{'dedupkey'}} = 1;
            $rc->{'first_author'} = $duplicates->{$rc->{'dedupkey'}}{'first_author'};
            $rc->{'first_author_uni'} = $duplicates->{$rc->{'dedupkey'}}{'first_author_uni'};
            my @list;
            foreach my $mra (sort (keys (%{$duplicates->{$rc->{'dedupkey'}}{'research_area'}}))) {
                if ($duplicates->{$rc->{'dedupkey'}}{'research_area'}{$mra} == 1) {
                    push (@list, $mra);
                } else {
                    push (@list, $mra . ' x ' . $duplicates->{$rc->{'dedupkey'}}{'research_area'}{$mra});
                }
            }
            $rc->{'research_area'} = join (', ', @list);
            $rc->{'doi'} = join (', ', sort (keys (%{$duplicates->{$rc->{'dedupkey'}}{'doi'}})));
            $rc->{'issn'} = join (', ', sort (keys (%{$duplicates->{$rc->{'dedupkey'}}{'issn'}})));
            if ($duplicates->{$rc->{'dedupkey'}}{'jtitle'}) {
                $rc->{'jtitle'} = $duplicates->{$rc->{'dedupkey'}}{'jtitle'};
            } elsif ($duplicates->{$rc->{'dedupkey'}}{'jtitle_alt'}) {
                $rc->{'jtitle'} = $duplicates->{$rc->{'dedupkey'}}{'jtitle_alt'};
            } elsif ($duplicates->{$rc->{'dedupkey'}}{'series'}) {
                $rc->{'jtitle'} = $duplicates->{$rc->{'dedupkey'}}{'series'};
            } else {
                $rc->{'jtitle'} = '';
            }
            delete ($rc->{'jtitle_alt'});
            delete ($rc->{'series'});
            $rc->{'source_id'} = join (', ', sort (@{$duplicates->{$rc->{'dedupkey'}}{'source_id'}}));
            $rc->{'class'} = join (', ', sort (@{$duplicates->{$rc->{'dedupkey'}}{'class'}}));
            $rc->{'class_reasons'} = join (', ', sort (@{$duplicates->{$rc->{'dedupkey'}}{'class_reasons'}}));
            $rc->{'bfi_class'} = join (', ', sort (keys (%{$duplicates->{$rc->{'dedupkey'}}{'bfi_class'}})));
            $rc->{'bfi_level'} = join (', ', sort (keys (%{$duplicates->{$rc->{'dedupkey'}}{'bfi_level'}})));
            foreach my $src (qw(aau au cbs dtu itu ku ruc sdu)) {
                $rc->{'source_' . $src} = $duplicates->{$rc->{'dedupkey'}}{'source_' . $src};
            }
            my @links = ();
            foreach my $id (sort (@{$duplicates->{$rc->{'dedupkey'}}{'source_id'}})) {
                my ($source, $source_id) = split (':', $id);
                push (@links, $self->cris_link ($source, $source_id));
            }
            $rc->{'cris_link'} = join (' ', @links);
        } else {
            $rc->{'issn'} = $self->display_issn ($rc->{'issn'});
            if ($rc->{'eissn'} =~ m/^[0-9]{7}[0-9Xx]/) {
                if ($rc->{'issn'}) {
                    $rc->{'issn'} .= ', ' . $self->display_issn ($rc->{'eissn'});
                } else {
                    $rc->{'issn'} = $self->display_issn ($rc->{'eissn'});
                }
            }
            delete ($rc->{'eissn'});
            if (!$rc->{'jtitle'}) {
                if ($rc->{'jtitle_alt'}) {
                    $rc->{'jtitle'} = $rc->{'jtitle_alt'};
                } elsif ($rc->{'series'}) {
                    $rc->{'jtitle'} = $rc->{'series'};
                } else {
                    $rc->{'jtitle'} = '';
                }
            }
            delete ($rc->{'jtitle_alt'});
            delete ($rc->{'series'});
            $rc->{'first_author_uni'} = $rc->{'source'};
            $rc->{'source_' . $rc->{'source'}} = 'X';
            $rc->{'cris_link'} = $self->cris_link ($rc->{'source'}, $rc->{'source_id'});
            $rc->{'source_id'} = $rc->{'source'} . ':' . $rc->{'source_id'};
            $rc->{'class'} = '';
            $rc->{'class_reasons'} = '';
            delete ($rc->{'source'});
            if ($rc->{'bfi_level'} == 0) {
                $rc->{'bfi_level'} = '';
            }
        }
        if ($rc->{'pub_class'} eq 'realized') {
            foreach my $fld (qw(realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre)) {
                if ($realised->{$rc->{'dedupkey'}}{$fld}) {
                    $rc->{$fld} = 1;
                }
            }
        }
        $rc->{'ddf_link'} = 'http://forskningsdatabasen.dk/en/catalog/' . $rc->{'dedupkey'};
        push (@{$self->{'result'}{'response'}{'body'}{'publication'}}, $rc);
    }
    $self->respond ();
}

sub comm_screened
{
    my ($self, $db) = @_;

    my $duplicates = $self->duplicates ($db, allRecords => 1);
    my $rs = $db->select ('title,first_author,source,research_area,doi,type,level,review,issn,eissn,scoped_type,scoped_review,scoped_level,screened_issn,' .
                          'bfi_class,bfi_level,source_id,dedupkey',
                          'records', 'scoped=0 or (screened_year=1 and screened_issn=0)', 'order by title');
    $self->{'result'}{'response'}{'body'}{'record'} = [];
    my $rc;
    while ($rc = $db->next ($rs)) {
        if ($rc->{'issn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            $rc->{'issn'} = substr ($rc->{'issn'}, 0, 4) . '-' . uc (substr ($rc->{'issn'}, 4));
        }
        if ($rc->{'eissn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            if ($rc->{'issn'}) {
                $rc->{'issn'} .= ', ' . substr ($rc->{'eissn'}, 0, 4) . '-' . uc (substr ($rc->{'eissn'}, 4));
            } else {
                $rc->{'issn'} = substr ($rc->{'eissn'}, 0, 4) . '-' . uc (substr ($rc->{'eissn'}, 4));
            }
        } else {
            if ($rc->{'eissn'} =~ m/[0-9Xx]/) {
                if ($rc->{'issn'}) {
                    $rc->{'issn'} .= ', ' . $rc->{'eissn'};
                } else {
                    $rc->{'issn'} = $rc->{'eissn'};
                }
            }
        }
        delete ($rc->{'eissn'});
        foreach my $fld (qw(scoped_type scoped_review scoped_level screened_issn)) {
            if ($rc->{$fld}) {
                $rc->{$fld} = '';
            } else {
                $rc->{$fld} = 'X';
            }
        }
        if ($rc->{'bfi_level'} == 0) {
            $rc->{'bfi_level'} = '';
        }
        $rc->{'cris_link'} = $self->cris_link ($rc->{'source'}, $rc->{'source_id'});
        if (exists ($duplicates->{$rc->{'dedupkey'}})) {
            my @id = ();
            foreach my $id (@{$duplicates->{$rc->{'dedupkey'}}{'ids'}}) {
                if ($id ne $rc->{'source_id'}) {
                    push (@id, $id);
                }
            }
            $rc->{'duplicates'} = join (', ', @id);
        } else {
            $rc->{'duplicates'} = '';
        }
        $rc->{'ddf_link'} = 'http://forskningsdatabasen.dk/en/catalog/' . $rc->{'dedupkey'};
        push (@{$self->{'result'}{'response'}{'body'}{'record'}}, $rc);
    }
    $self->respond ();
}

sub comm_extra
{
    my ($self, $db) = @_;

    my $duplicates = $self->duplicates ($db, allRecords => 1);
    my $rs = $db->select ('title,first_author,source,research_area,doi,issn,eissn,pubyear,year,bfi_class,bfi_level,source_id,dedupkey',
                          'records', 'scoped=1 and screened_year=0 and screened_issn=1', 'order by title');
    $self->{'result'}{'response'}{'body'}{'record'} = [];
    my $rc;
    while ($rc = $db->next ($rs)) {
        if ($rc->{'issn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            $rc->{'issn'} = substr ($rc->{'issn'}, 0, 4) . '-' . uc (substr ($rc->{'issn'}, 4));
        }
        if ($rc->{'eissn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            if ($rc->{'issn'}) {
                $rc->{'issn'} .= ', ' . substr ($rc->{'eissn'}, 0, 4) . '-' . uc (substr ($rc->{'eissn'}, 4));
            } else {
                $rc->{'issn'} = substr ($rc->{'eissn'}, 0, 4) . '-' . uc (substr ($rc->{'eissn'}, 4));
            }
        } else {
            if ($rc->{'eissn'} =~ m/[0-9Xx]/) {
                if ($rc->{'issn'}) {
                    $rc->{'issn'} .= ', ' . $rc->{'eissn'};
                } else {
                    $rc->{'issn'} = $rc->{'eissn'};
                }
            }
        }
        delete ($rc->{'eissn'});
        if ($rc->{'bfi_level'} == 0) {
            $rc->{'bfi_level'} = '';
        }
        $rc->{'cris_link'} = $self->cris_link ($rc->{'source'}, $rc->{'source_id'});
        if (exists ($duplicates->{$rc->{'dedupkey'}})) {
            my @id = ();
            foreach my $id (@{$duplicates->{$rc->{'dedupkey'}}{'ids'}}) {
                if ($id ne $rc->{'source_id'}) {
                    push (@id, $id);
                }
            }
            $rc->{'duplicates'} = join (', ', @id);
        } else {
            $rc->{'duplicates'} = '';
        }
        $rc->{'ddf_link'} = 'http://forskningsdatabasen.dk/en/catalog/' . $rc->{'dedupkey'};
        push (@{$self->{'result'}{'response'}{'body'}{'record'}}, $rc);
    }
    $self->respond ();
}

sub comm_record
{
    my ($self, $db, $id) = @_;

    my $duplicates = $self->duplicates ($db);
    my $rs = $db->select ('*', 'records', "source_id='$id'");
    my $rc;
    if ($rc = $db->next ($rs)) {
        $rc->{'status'} = 'found';
        if ($rc->{'issn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            $rc->{'issn'} = substr ($rc->{'issn'}, 0, 4) . '-' . uc (substr ($rc->{'issn'}, 4));
        }
        if ($rc->{'eissn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            $rc->{'eissn'} = substr ($rc->{'eissn'}, 0, 4) . '-' . uc (substr ($rc->{'eissn'}, 4));
        }
        if ($rc->{'doaj_issn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            $rc->{'doaj_issn'} = substr ($rc->{'doaj_issn'}, 0, 4) . '-' . uc (substr ($rc->{'doaj_issn'}, 4));
        }
        if ($rc->{'romeo_issn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            $rc->{'romeo_issn'} = substr ($rc->{'romeo_issn'}, 0, 4) . '-' . uc (substr ($rc->{'romeo_issn'}, 4));
        }
        $rc->{'cris_link'} = $self->cris_link ($rc->{'source'}, $rc->{'source_id'});
        if (exists ($duplicates->{$rc->{'dedupkey'}})) {
            my @id = ();
            foreach my $id (@{$duplicates->{$rc->{'dedupkey'}}{'ids'}}) {
                if ($id ne $rc->{'source_id'}) {
                    push (@id, $id);
                }
            }
            $rc->{'duplicates'} = [@id];
        } else {
            $rc->{'duplicates'} = '';
        }
        delete ($rc->{'mods'});
        delete ($rc->{'original_xml'});
    } else {
        $self->error ("No record found with ID: $id");
    }
    $rs = $db->select ('oai_harvest,oai_datestamp,jno,pno,jtitle,jtitle_alt', 'mxd', "source_id='$id'");
    my $rec;
    if ($rec = $db->next ($rs)) {
        foreach my $f (qw(oai_harvest oai_datestamp jno pno jtitle jtitle_alt)) {
            $rc->{$f} = $rec->{$f};
        }
    }
    $self->{'result'}{'response'}{'body'}{'record'} = $rc;
    $self->respond ();
}

sub comm_recordMXD
{
    my ($self, $db, $id) = @_;

    my $rs = $db->select ('original_xml', 'mxd', "source_id='$id'");
    my $rc;
    if ($rc = $db->next ($rs)) {
        $self->{'result'}{'response'}{'body'}{'record'} = {
            status => 'found',
            mxd => $rc->{'original_xml'},
        };
    } else {
        $self->error ("No record found with ID: $id");
    }
    $self->respond ();
}

sub comm_recordMXDraw
{
    my ($self, $db, $id) = @_;

    print ("Content-Type: text/xml\n\n");
    my $rs = $db->select ('original_xml', 'mxd', "source_id='$id'");
    my $rc;
    if ($rc = $db->next ($rs)) {
        print ($rc->{'original_xml'});
    } else {
        print ("<error>No record found with ID: $id</error>");
    }
    exit (0);
}

sub comm_publication
{
    my ($self, $db, $id) = @_;

    my $duplicates = $self->duplicates ($db);
    my $rs = $db->select ('*', 'records', "source_id='$id'");
    my $rc;
    if ($rc = $db->next ($rs)) {
        $rc->{'status'} = 'found';
        if ($rc->{'issn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            $rc->{'issn'} = substr ($rc->{'issn'}, 0, 4) . '-' . uc (substr ($rc->{'issn'}, 4));
        }
        if ($rc->{'eissn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            $rc->{'eissn'} = substr ($rc->{'eissn'}, 0, 4) . '-' . uc (substr ($rc->{'eissn'}, 4));
        }
        if ($rc->{'doaj_issn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            $rc->{'doaj_issn'} = substr ($rc->{'doaj_issn'}, 0, 4) . '-' . uc (substr ($rc->{'doaj_issn'}, 4));
        }
        if ($rc->{'romeo_issn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            $rc->{'romeo_issn'} = substr ($rc->{'romeo_issn'}, 0, 4) . '-' . uc (substr ($rc->{'romeo_issn'}, 4));
        }
        $rc->{'cris_link'} = $self->cris_link ($rc->{'source'}, $rc->{'source_id'});
        if (exists ($duplicates->{$rc->{'dedupkey'}})) {
            my @id = ();
            foreach my $id (@{$duplicates->{$rc->{'dedupkey'}}{'ids'}}) {
                if ($id ne $rc->{'source_id'}) {
                    push (@id, $id);
                }
            }
            $rc->{'duplicates'} = [@id];
        }
        delete ($rc->{'mods'});
        delete ($rc->{'original_xml'});
    } else {
        $self->error ("No record found with ID: $id");
    }
    $rs = $db->select ('oai_harvest,oai_datestamp,jno,pno,jtitle,jtitle_alt', 'mxd', "source_id='$id'");
    my $rec;
    if ($rec = $db->next ($rs)) {
        foreach my $f (qw(oai_harvest oai_datestamp jno pno jtitle jtitle_alt)) {
            $rc->{$f} = $rec->{$f};
        }
    }
    $self->{'result'}{'response'}{'body'}{'record'} = $rc;


    $duplicates = $self->duplicates ($db, merge => 1);
    $rs = $db->select ('title,first_author,source,research_area,doi,issn,eissn,class,bfi_class,bfi_level,source_id,dedupkey',
                       'records', 'scoped=1 and screened=1');
    $self->{'result'}{'response'}{'body'}{'record'} = [];
    my $done = {};
    while ($rc = $db->next ($rs)) {
        if ($done->{$rc->{'dedupkey'}}) {
            next;
        }
        if ($duplicates->{$rc->{'dedupkey'}}) {
            $done->{$rc->{'dedupkey'}} = 1;
            $rc->{'first_author'} = $duplicates->{$rc->{'dedupkey'}}{'first_author'};
            $rc->{'first_author_uni'} = $duplicates->{$rc->{'dedupkey'}}{'first_author_uni'};
            $rc->{'research_area'} = join (', ', sort (keys (%{$duplicates->{$rc->{'dedupkey'}}{'research_area'}})));
            $rc->{'doi'} = join (', ', sort (keys (%{$duplicates->{$rc->{'dedupkey'}}{'doi'}})));
            $rc->{'issn'} = join (', ', sort (keys (%{$duplicates->{$rc->{'dedupkey'}}{'issn'}})));
            $rc->{'source_id'} = join (', ', sort (@{$duplicates->{$rc->{'dedupkey'}}{'source_id'}}));
            $rc->{'class'} = join (', ', sort (@{$duplicates->{$rc->{'dedupkey'}}{'class'}}));
            $rc->{'bfi_class'} = join (', ', sort (keys (%{$duplicates->{$rc->{'dedupkey'}}{'bfi_class'}})));
            $rc->{'bfi_level'} = join (', ', sort (keys (%{$duplicates->{$rc->{'dedupkey'}}{'bfi_level'}})));
            foreach my $src (qw(aau au cbs dtu itu ku ruc sdu)) {
                $rc->{'source_' . $src} = $duplicates->{$rc->{'dedupkey'}}{'source_' . $src};
            }
            my @links = ();
            foreach my $id (sort (@{$duplicates->{$rc->{'dedupkey'}}{'source_id'}})) {
                my ($source, $source_id) = split (':', $id);
                push (@links, $self->cris_link ($source, $source_id));
            }
            $rc->{'cris_link'} = join (' ', @links);
        } else {
            $rc->{'issn'} = $self->display_issn ($rc->{'issn'});
            if ($rc->{'eissn'} =~ m/^[0-9]{7}[0-9Xx]/) {
                if ($rc->{'issn'}) {
                    $rc->{'issn'} .= ', ' . $self->display_issn ($rc->{'eissn'});
                } else {
                    $rc->{'issn'} = $self->display_issn ($rc->{'eissn'});
                }
            }
            delete ($rc->{'eissn'});
            $rc->{'first_author_uni'} = $rc->{'source'};
            $rc->{'source_' . $rc->{'source'}} = 'X';
            $rc->{'cris_link'} = $self->cris_link ($rc->{'source'}, $rc->{'source_id'});
            $rc->{'source_id'} = $rc->{'source'} . ':' . $rc->{'source_id'};
            delete ($rc->{'source'});
            if ($rc->{'bfi_level'} == 0) {
                $rc->{'bfi_level'} = '';
            }
        }
        push (@{$self->{'result'}{'response'}{'body'}{'record'}}, $rc);
    }
    $self->respond ();
}

sub comm_whitelist
{
    my ($self, $db) = @_;
    my $ret = {};

    $self->{'result'}{'response'}{'body'}{'record'} = [];
    my $rs = $db->select ('code,name,type,uri,domain,path,proposer,usage_records', 'doar', '', 'order by code');
    my $rc;
    while ($rc = $db->next ($rs)) {
        push (@{$self->{'result'}{'response'}{'body'}{'record'}}, $rc);
    }
    $self->respond ();
}

sub comm_blacklist
{
    my ($self, $db) = @_;
    my $ret = {};

    $self->{'result'}{'response'}{'body'}{'record'} = [];
    my $rs = $db->select ('title,pissn,eissn,publisher,embargo,url,usage_records,usage_effective', 'jwlep');
    my $rc;
    my $records = {};
    while ($rc = $db->next ($rs)) {
        if ($rc->{'pissn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            $rc->{'pissn'} = substr ($rc->{'pissn'}, 0, 4) . '-' . uc (substr ($rc->{'pissn'}, 4));
        }
        if ($rc->{'eissn'} =~ m/^[0-9]{7}[0-9Xx]/) {
            $rc->{'eissn'} = substr ($rc->{'eissn'}, 0, 4) . '-' . uc (substr ($rc->{'eissn'}, 4));
        }
        my $key = lc ($rc->{'title'});
        $key =~ s/[^0-9a-z]+/ /g;
        $key =~ s/\s\s+/ /g;
        $key =~ s/^\s+//;
        $key =~ s/\s+$//;
        if (exists ($records->{$key})) {
            push (@{$records->{$key}}, $rc);
        } else {
            $records->{$key} = [$rc];
        }
    }
    foreach my $key (sort (keys (%{$records}))) {
        push (@{$self->{'result'}{'response'}{'body'}{'record'}}, @{$records->{$key}});
    }
    $self->respond ();
}

sub duplicates
{
    my ($self, $db, %opt) = @_;

    my $cachekey = $self->{'dbkey'};
    if ($opt{'allRecords'}) {
        $cachekey .= '.allRecords';
    }
    if ($opt{'merge'}) {
        $cachekey .= '.merge';
    }
    if (exists ($self->{'cache'}{$cachekey}{'duplicates'})) {
        return ($self->{'cache'}{$cachekey}{'duplicates'});
    }
    my $duplicates = {};
    my $rec = {key => '', n => 0, ids => [], class => ''};
    my $where;
    if ($opt{'allRecords'}) {
        $where = '';
    } else {
        $where = 'scoped=1 and screened=1';
    }
    my $rs;
    if ($opt{'merge'}) {
        $rs = $db->select ('dedupkey,source_id,first_author,first_author_pos,research_area,doi,issn,eissn,jtitle,jtitle_alt,series,source,class,class_reasons,bfi_class,bfi_level', 'records',
                           $where, 'order by dedupkey');
    } else {
        $rs = $db->select ('dedupkey,source_id', 'records', $where, 'order by dedupkey');
    }
    my $rc;
    while ($rc = $db->next ($rs)) {
        if ($rc->{'dedupkey'} eq $rec->{'key'}) {
            push (@{$rec->{'ids'}}, $rc->{'source_id'});
            $rec->{'n'}++;
        } else {
            if ($rec->{'n'} > 1) {
                $duplicates->{$rec->{'key'}} = $rec;
            }
            $rec = {key => $rc->{'dedupkey'}, n => 1, ids => [$rc->{'source_id'}], class => ''};
        }
        if ($opt{'merge'}) {
            if ((defined ($rc->{'first_author_pos'})) && ($rc->{'first_author_pos'} > 0)) {
                if ((!exists ($rec->{'first_author'})) || ($rc->{'first_author_pos'} < $rec->{'first_author_pos'})) {
                    $rec->{'first_author'} = $rc->{'first_author'};
                    $rec->{'first_author_pos'} = $rc->{'first_author_pos'};
                    $rec->{'first_author_uni'} = $rc->{'source'};
                }
            }
            $rec->{'research_area'}{$rc->{'research_area'}} = 1;
            if ($rc->{'doi'}) {
                $rec->{'doi'}{$rc->{'doi'}} = 1;
            }
            if ($rc->{'issn'}) {
                $rec->{'issn'}{$self->display_issn ($rc->{'issn'})} = 1;
            }
            if ($rc->{'eissn'}) {
                $rec->{'issn'}{$self->display_issn ($rc->{'eissn'})} = 1;
            }
            if ((!$rec->{'jtitle'}) && ($rc->{'jtitle'} =~ m/[A-Za-z]/)) {
                $rec->{'jtitle'} = $rc->{'jtitle'};
            }
            if ((!$rec->{'jtitle_alt'}) && ($rc->{'jtitle_alt'} =~ m/[A-Za-z]/)) {
                $rec->{'jtitle_alt'} = $rc->{'jtitle_alt'};
            }
            if ((!$rec->{'series'}) && ($rc->{'series'} =~ m/[A-Za-z]/)) {
                $rec->{'series'} = $rc->{'series'};
            }
            $rec->{'source_' . $rc->{'source'}} = 'X';
            if ($rec->{'source_id'}) {
                push (@{$rec->{'source_id'}}, $rc->{'source'} . ':' . $rc->{'source_id'});
            } else {
                $rec->{'source_id'} = [$rc->{'source'} . ':' . $rc->{'source_id'}];
            }
            if ($rec->{'class'}) {
                push (@{$rec->{'class'}}, $rc->{'source'} . ':' . $rc->{'class'});
            } else {
                $rec->{'class'} = [$rc->{'source'} . ':' . $rc->{'class'}];
            }
            if ($rec->{'class_reasons'}) {
                push (@{$rec->{'class_reasons'}}, $rc->{'source'} . ':' . $rc->{'class_reasons'});
            } else {
                $rec->{'class_reasons'} = [$rc->{'source'} . ':' . $rc->{'class_reasons'}];
            }
            if ($rc->{'bfi_class'}) {
                $rec->{'bfi_class'}{$rc->{'bfi_class'}} = 1;
            }
            if ($rc->{'bfi_level'}) {
                $rec->{'bfi_level'}{$rc->{'bfi_level'}} = 1;
            }
        }
    }
    if ($rec->{'n'} > 1) {
        $duplicates->{$rec->{'key'}} = $rec;
    }
    return ($self->{'cache'}{$cachekey}{'duplicates'} = $duplicates);
}

sub cris_link
{
    my ($self, $source, $id) = @_;

    $source = lc ($source);
    $source =~ s/[^a-z]//g;
    if ($source eq 'aau') {
        return ("http://vbn.aau.dk/en/publications/id($id).html");
    }
    if ($source eq 'au') {
        return ("http://pure.au.dk/portal/en/publications/id($id).html");
    }
    if ($source eq 'cbs') {
        return ("http://research.cbs.dk/en/publications/id($id).html");
    }
    if ($source eq 'dtu') {
        return ("http://orbit.dtu.dk/en/publications/id($id).html");
    }
    if ($source eq 'itu') {
        return ("https://pure.itu.dk/portal/en/publications/id($id).html");
    }
    if ($source eq 'ku') {
        return ("http://research.ku.dk/search/?pure=en/publications/id($id).html");
    }
    if ($source eq 'ruc') {
        return ("https://rucforsk.ruc.dk/site/en/publications/id($id).html");
    }
    if ($source eq 'sdu') {
        return ("http://findresearcher.sdu.dk/portal/da/publications/id($id).html");
    }
    die ("fatal: cris_link - unsupported source: $source");
}

sub oai_link
{
    my ($self, $source, $id) = @_;

    $source = lc ($source);
    $source =~ s/[^a-z]//g;
    if ($source eq 'aau') {
        return ("http://vbn.aau.dk/ws/oai?verb=GetRecord&metadataPrefix=ddf-mxd&identifier=oai:pure.atira.dk:publications/$id");
    }
    if ($source eq 'au') {
        return ("https://pure.au.dk/ws/oai?verb=GetRecord&metadataPrefix=ddf-mxd&identifier=oai:pure.atira.dk:publications/$id");
    }
    if ($source eq 'cbs') {
        return ("http://research.cbs.dk/ws/oai?verb=GetRecord&metadataPrefix=ddf-mxd&identifier=oai:pure.atira.dk:publications/$id");
    }
    if ($source eq 'dtu') {
        return ("http://orbit.dtu.dk/ws/oai?verb=GetRecord&metadataPrefix=ddf-mxd&identifier=oai:pure.atira.dk:publications/$id");
    }
    if ($source eq 'itu') {
        return ("https://pure.itu.dk/ws/oai?verb=GetRecord&metadataPrefix=ddf-mxd&identifier=oai:pure.atira.dk:publications/$id");
    }
    if ($source eq 'ku') {
        return ("http://curis.ku.dk/ws/oai?verb=GetRecord&metadataPrefix=ddf-mxd&identifier=oai:pure.atira.dk:publications/$id");
    }
    if ($source eq 'ruc') {
        return ("https://rucforsk.ruc.dk/ws/oai?verb=GetRecord&metadataPrefix=ddf-mxd&identifier=oai:pure.atira.dk:publications/$id");
    }
    if ($source eq 'sdu') {
        return ("http://findresearcher.sdu.dk:8080/ws/oai?verb=GetRecord&metadataPrefix=ddf-mxd&identifier=oai:pure.atira.dk:publications/$id");
    }
    die ("fatal: cris_link - unsupported source: $source");
}

sub respond
{
    my ($self) = @_;
    my $result;

    $self->{'result'}{'response'}{'datestamp'} = 'responseDatestamp';
    $self->{'result'}{'response'}{'elapse'} = 'responseElapse';
    if ($self->{'format'} eq 'json') {
        print ("Content-Type: application/json\n\n");
        $result = encode_json ({oa_indicator => $self->{'result'}});
    } elsif ($self->{'format'} eq 'csv') {
        print ("Content-Type: text/csv\n\n");
        $result = $self->respond_csv_encode ($self->{'result'});
    } elsif ($self->{'format'} eq 'html') {
        print ("Content-Type: text/html\n\n");
        $result = $self->respond_html_encode ($self->{'result'});
    } else {
        print ("Content-Type: text/xml\n\n");
        $result = '<?xml version="1.0"?>' . "\n";
        $result .= '<oa_indicator>';
        $result .= $self->respond_xml_encode ('', $self->{'result'});
        $result .= '</oa_indicator>';
    }
    if (!$self->{'template'}) {
        $result =~ s/responseDatestamp/$self->date (time)/e;
        $result =~ s/responseElapse/sprintf ('%0.6f', time - $self->{'start'})/e;
    }
    print ($result);
    exit (0);
}

sub cache
{
    my ($self, $file) = @_;
    my $result;

    if ((!$self->{'run'}) || ($self->{'run'} eq 'latest')) {
        $self->{'run'} = '';
    }
    my $dbkey = $self->{'year'} . '-' . $self->{'type'} . '-' . $self->{'run'};
    my $db = new OA::Indicator::DB;
    $db->reuse ($self->{'year'}, $self->{'type'}, $self->{'run'});
    my $rundir = '/var/lib/oa-indicator/runs/' . $db->id;
    if (-e "$rundir/cache/$self->{'comm'}.$self->{'format'}") {
        my $fin;
        if (!open ($fin, "$rundir/cache/$self->{'comm'}.$self->{'format'}")) {
            return;
        }
        my $result = join ('', <$fin>);
        close ($fin);
        if ($self->{'format'} =~ m/(json|xml)/) {
            $result =~ s/requestDatestamp/$self->date ($self->{'start'})/e;
            $result =~ s/responseDatestamp/$self->date (time)/e;
            $result =~ s/responseElapse/sprintf ('%0.6f', time - $self->{'start'})/e;
        }
        if ($self->{'format'} eq 'json') {
            print ("Content-Type: application/json\n\n");
            print ($result);
            exit (0);
        }
        if ($self->{'format'} eq 'csv') {
            print ("Content-Type: text/csv\n\n");
            print ($result);
            exit (0);
        }
        if ($self->{'format'} eq 'xml') {
            print ("Content-Type: text/xml\n\n");
            print ($result);
            exit (0);
        }
    }
}

sub respond_csv_encode
{
    my ($self, $data) = @_;

    my $result = '';
    if ($self->{'comm'} eq 'national') {
        my @cols = ('');
        foreach my $class (qw(realized realized-gre-loc realized-gre-ext realized-gol-apc realized-gol-fre unused unclear total total-clear
                              relative-realized relative-realized-gre-loc relative-realized-gre-ext relative-realized-gol-apc relative-realized-gol-fre
                              relative-unused relative-unclear
                              relative-clear-realized relative-clear-realized-gre-loc relative-clear-realized-gre-ext relative-clear-realized-gol-apc
                              relative-clear-realized-gol-fre relative-clear-unused)) {
            my $s = $class;
            $s =~ s/-/ /g;
            $s =~ s/^([a-z])/uc ($1)/ge;
            $s =~ s/( [a-z])/uc ($1)/ge;
            push (@cols, $s);
        }
        $result .= join ("\t", @cols) . "\n";
        @cols = ('all'); 
        foreach my $class (qw(realized realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre unused unclear)) {
            push (@cols, $self->{'result'}{'response'}{'body'}{$class});
        }
        push (@cols, $self->{'result'}{'response'}{'body'}{'total'});
        push (@cols, $self->{'result'}{'response'}{'body'}{'total_clear'});
        foreach my $class (qw(realized realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre unused unclear)) {
            push (@cols, $self->{'result'}{'response'}{'body'}{'relative'}{$class});
        }
        foreach my $class (qw(realized realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre unused)) {
            push (@cols, $self->{'result'}{'response'}{'body'}{'relative_clear'}{$class});
        }
        $result .= join ("\t", @cols) . "\n";
        return ($result);
    }
    if ($self->{'comm'} eq 'research_area') {
        my @cols = ('');
        foreach my $class (qw(realized realized-gre-loc realized-gre-ext realized-gol-apc realized-gol-fre unused unclear total total-clear
                              relative-realized relative-realized-gre-loc relative-realized-gre-ext relative-realized-gol-apc relative-realized-gol-fre
                              relative-unused relative-unclear
                              relative-clear-realized relative-clear-realized-gre-loc relative-clear-realized-gre-ext relative-clear-realized-gol-apc
                              relative-clear-realized-gol-fre relative-clear-unused)) {
            my $s = $class;
            $s =~ s/-/ /g;
            $s =~ s/^([a-z])/uc ($1)/ge;
            $s =~ s/( [a-z])/uc ($1)/ge;
            push (@cols, $s);
        }
        $result .= join ("\t", @cols) . "\n";
        foreach my $ra (qw(hum soc sci med)) {
            @cols = ($ra);
            foreach my $class (qw(realized realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre unused unclear)) {
                push (@cols, $self->{'result'}{'response'}{'body'}{$ra}{$class});
            }
            push (@cols, $self->{'result'}{'response'}{'body'}{$ra}{'total'});
            push (@cols, $self->{'result'}{'response'}{'body'}{$ra}{'total_clear'});
            foreach my $class (qw(realized realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre unused unclear)) {
                push (@cols, $self->{'result'}{'response'}{'body'}{$ra}{'relative'}{$class});
            }
            foreach my $class (qw(realized realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre unused)) {
                push (@cols, $self->{'result'}{'response'}{'body'}{$ra}{'relative_clear'}{$class});
            }
            $result .= join ("\t", @cols) . "\n";
        }
        return ($result);
    }
    if ($self->{'comm'} eq 'universities') {
        my @cols = ('');
        foreach my $class (qw(realized realized-gre-loc realized-gre-ext realized-gol-apc realized-gol-fre unused unclear total total-clear
                              relative-realized relative-realized-gre-loc relative-realized-gre-ext relative-realized-gol-apc relative-realized-gol-fre
                              relative-unused relative-unclear
                              relative-clear-realized relative-clear-realized-gre-loc relative-clear-realized-gre-ext relative-clear-realized-gol-apc
                              relative-clear-realized-gol-fre relative-clear-unused)) {
            my $s = $class;
            $s =~ s/-/ /g;
            $s =~ s/^([a-z])/uc ($1)/ge;
            $s =~ s/( [a-z])/uc ($1)/ge;
            push (@cols, $s);
        }
        $result .= join ("\t", @cols) . "\n";
        foreach my $uni (qw(aau au cbs dtu itu ku ruc sdu)) {
            @cols = ($uni);
            foreach my $class (qw(realized realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre unused unclear)) {
                push (@cols, $self->{'result'}{'response'}{'body'}{$uni}{$class});
            }
            push (@cols, $self->{'result'}{'response'}{'body'}{$uni}{'total'});
            push (@cols, $self->{'result'}{'response'}{'body'}{$uni}{'total_clear'});
            foreach my $class (qw(realized realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre unused unclear)) {
                push (@cols, $self->{'result'}{'response'}{'body'}{$uni}{'relative'}{$class});
            }
            foreach my $class (qw(realized realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre unused)) {
                push (@cols, $self->{'result'}{'response'}{'body'}{$uni}{'relative_clear'}{$class});
            }
            $result .= join ("\t", @cols) . "\n";
        }
        return ($result);
    }
    if ($self->{'comm'} eq 'records') {
        my $csv = new Text::CSV ({binary => 1, sep_char => "\t"});
        my $fields = {
            title                  => 'Title',
            first_author           => 'First (Local) Author',
            source                 => 'University',
            research_area          => 'MRA',
            doi                    => 'DOI',
            issn                   => 'ISSN',
            jtitle                 => 'Journal title / Series',
            class                  => 'Status',
            realized_gre_loc       => 'Green local',
            realized_gre_ext       => 'Green ext.',
            realized_gol_apc       => 'Golden w/APC',
            realized_gol_fre       => 'Golden wo/APC',
            class_reasons          => 'Reason',
            bfi_class              => 'Classification',
            bfi_level              => 'Level',
            dedupkey               => 'DDF ID',
            ddf_link               => 'DDF Link',
            source_id              => 'CRIS ID',
            cris_link              => 'University CRIS Record',
            duplicates             => 'Duplicates',
            fulltext_remote_valid  => 'External repository',
            blacklisted_issn       => 'Blacklisted',
            upw_rep                => 'Unpaywall repository',
            upw_pub                => 'Unpaywall publisher',
            upw_reasons            => 'Unpaywall reasons',
        };
        my @fields = qw(title first_author source research_area doi issn jtitle class realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre
                        class_reasons bfi_class bfi_level dedupkey ddf_link source_id cris_link duplicates fulltext_remote_valid blacklisted_issn
                        upw_rep upw_pub upw_reasons);
        my @cols = ();
        foreach my $fld (@fields) {
            push (@cols, $fields->{$fld});
        }
        $csv->print (*STDOUT, \@cols);
        print ("\n");
        foreach my $rec (@{$self->{'result'}{'response'}{'body'}{'record'}}) {
            @cols = ();
            foreach my $fld (@fields) {
                push (@cols, $rec->{$fld});
            }
            $csv->print (*STDOUT, \@cols);
            print ("\n");
        }
        return ($result);
    }
    if ($self->{'comm'} eq 'publications') {
        my $csv = new Text::CSV ({binary => 1, sep_char => "\t"});
        my $fields = {
            title            => 'Title',
            first_author     => 'First Danish Author',
            first_author_uni => 'University of first danish author',
            pub_research_area=> 'Main Research Area',
            bfi_research_area=> 'BFI Research Area',
            research_area    => 'Research Area details',
            doi              => 'DOI',
            issn             => 'ISSN',
            jtitle           => 'Journal title / Series',
            source_aau       => 'AAU',
            source_au        => 'AU',
            source_cbs       => 'CBS',
            source_dtu       => 'DTU',
            source_itu       => 'ITU',
            source_ku        => 'KU',
            source_ruc       => 'RUC',
            source_sdu       => 'SDU',
            pub_class        => 'Status',
            realized_gre_loc => 'Green local',
            realized_gre_ext => 'Green ext.',
            realized_gol_apc => 'Golden w/APC',
            realized_gol_fre => 'Golden wo/APC',
            pub_class_reasons=> 'Status Reason',
            class            => 'Record class',
            class_reasons     => 'Record reason',
            bfi_class        => 'Classification',
            bfi_level        => 'Level',
            dedupkey         => 'DDF ID',
            ddf_link         => 'DDF Link',
            source_id        => 'IDs',
            cris_link        => 'University CRIS Records',
        };
        my @fld = qw(title first_author first_author_uni pub_research_area bfi_research_area research_area doi issn jtitle
                     source_aau source_au source_cbs source_dtu source_itu source_ku source_ruc source_sdu
                     pub_class realized_gre_loc realized_gre_ext realized_gol_apc realized_gol_fre pub_class_reasons class class_reasons
                     bfi_class bfi_level dedupkey ddf_link source_id cris_link);
        my @cols = ();
        foreach my $fld (@fld) {
            push (@cols, $fields->{$fld});
        }
        $csv->print (*STDOUT, \@cols);
        print ("\n");
        foreach my $rec (@{$self->{'result'}{'response'}{'body'}{'publication'}}) {
            @cols = ();
            foreach my $fld (@fld) {
                push (@cols, $rec->{$fld});
            }
            $csv->print (*STDOUT, \@cols);
            print ("\n");
        }
        return ($result);
    }
    if ($self->{'comm'} eq 'screened') {
        my $csv = new Text::CSV ({binary => 1, sep_char => "\t"});
        my $fields = {
            title         => 'Title',
            first_author  => 'First (Local) Author',
            source        => 'Uni',
            research_area => 'MRA',
            doi           => 'DOI',
            type          => 'doc_type',
            review        => 'doc_review',
            level         => 'doc_level',
            issn          => 'ISSN',
            scoped_type   => 'Scoped Type',
            scoped_review => 'Scoped Review',
            scoped_level  => 'Scoped Level',
            screened_issn => 'Screened ISSN',
            bfi_class     => 'Classification',
            bfi_level     => 'Level',
            dedupkey      => 'DDF-ID',
            ddf_link      => 'DDF-Link',
            source_id     => 'CRIS-ID',
            cris_link     => 'CRIS-Link',
            duplicates    => 'Duplicates',
        };
        my @cols = ();
        foreach my $fld (qw(title first_author source research_area doi type review level issn scoped_type scoped_review scoped_level screened_issn bfi_class bfi_level dedupkey ddf_link source_id cris_link duplicates)) {
            push (@cols, $fields->{$fld});
        }
        $csv->print (*STDOUT, \@cols);
        print ("\n");
        foreach my $rec (@{$self->{'result'}{'response'}{'body'}{'record'}}) {
            @cols = ();
            foreach my $fld (qw(title first_author source research_area doi type review level issn scoped_type scoped_review scoped_level screened_issn bfi_class bfi_level dedupkey ddf_link source_id cris_link duplicates)) {
                push (@cols, $rec->{$fld});
            }
            $csv->print (*STDOUT, \@cols);
            print ("\n");
        }
        return ($result);
    }
    if ($self->{'comm'} eq 'extra') {
        my $csv = new Text::CSV ({binary => 1, sep_char => "\t"});
        my $fields = {
            title         => 'Title',
            first_author  => 'First (Local) Author',
            source        => 'Uni',
            research_area => 'MRA',
            doi           => 'DOI',
            issn          => 'ISSN',
            pubyear       => 'Pub. Year',
            year          => 'Report Year',
            bfi_class     => 'Classification',
            bfi_level     => 'Level',
            dedupkey      => 'DDF-ID',
            ddf_link      => 'DDF-Link',
            source_id     => 'CRIS-ID',
            cris_link     => 'CRIS-Link',
            duplicates    => 'Duplicates',
        };
        my @cols = ();
        foreach my $fld (qw(title first_author source research_area doi issn pubyear year bfi_class bfi_level dedupkey ddf_link source_id cris_link duplicates)) {
            push (@cols, $fields->{$fld});
        }
        $csv->print (*STDOUT, \@cols);
        print ("\n");
        foreach my $rec (@{$self->{'result'}{'response'}{'body'}{'record'}}) {
            @cols = ();
            foreach my $fld (qw(title first_author source research_area doi issn pubyear year bfi_class bfi_level dedupkey ddf_link source_id cris_link duplicates)) {
                push (@cols, $rec->{$fld});
            }
            $csv->print (*STDOUT, \@cols);
            print ("\n");
        }
        return ($result);
    }
    if ($self->{'comm'} eq 'whitelist') {
        my $csv = new Text::CSV ({binary => 1, sep_char => "\t"});
        my $fields = {
            code          => 'Code',
            name          => 'Name',
            type          => 'Type',
            uri           => 'URL',
            domain        => 'Host',
            path          => 'Path',
            proposer      => 'Proposer',
            usage_records => 'Usage-Records'
        };
        my @cols = ();
        foreach my $fld (qw(code name type uri domain path proposer usage_records)) {
            push (@cols, $fields->{$fld});
        }
        $csv->print (*STDOUT, \@cols);
        print ("\n");
        foreach my $rec (@{$self->{'result'}{'response'}{'body'}{'record'}}) {
            @cols = ();
            foreach my $fld (qw(code name type uri domain path proposer usage_records)) {
                push (@cols, $rec->{$fld});
            }
            $csv->print (*STDOUT, \@cols);
            print ("\n");
        }
        return ($result);
    }
    if ($self->{'comm'} eq 'blacklist') {
        my $csv = new Text::CSV ({binary => 1, sep_char => "\t"});
        my $fields = {
            title            => 'Journal title',
            pissn            => 'ISSN',
            eissn            => 'E-ISSN',
            publisher        => 'Publisher',
            embargo          => 'Embargo periode',
            url              => 'URL',
            usage_effective  => 'Usage-class',
            usage_records    => 'Usage-records',
        };
        my @cols = ();
        foreach my $fld (qw(title pissn eissn publisher embargo url usage_effective usage_records)) {
            push (@cols, $fields->{$fld});
        }
        $csv->print (*STDOUT, \@cols);
        print ("\n");
        foreach my $rec (@{$self->{'result'}{'response'}{'body'}{'record'}}) {
            @cols = ();
            foreach my $fld (qw(title pissn eissn publisher embargo url usage_effective usage_records)) {
                push (@cols, $rec->{$fld});
            }
            $csv->print (*STDOUT, \@cols);
            print ("\n");
        }
        return ($result);
    }
    return ("#CSV format unsupported for command: $self->{'comm'}");
}

sub respond_html_encode
{
    my ($self) = @_;

    if ((defined ($self->{'result'}{'response'}{'error'})) && ($self->{'result'}{'response'}{'error'} !~ m/^\s*$/)) {
        return ($self->scr_parse ($self->scr_file ('error'), $self->{'result'}{'response'}));
    }
    if ($self->{'comm'} eq 'record') {
        my $rec = $self->{'result'}{'response'}{'body'}{'record'};
        if ($rec->{'scoped'}) {
            $rec->{'scopeDetails'} = 'Record is in scope.';
        } else {
            $rec->{'scopeDetails'} = 'Record is not in scope because ';
            my @reasons = ();
            if (!$rec->{'scoped_type'}) {
                push (@reasons, 'it has the wrong type: <i>' . $rec->{'type'} . '</i>');
            }
            if (!$rec->{'scoped_level'}) {
                push (@reasons, 'it has the wrong level: <i>' . $rec->{'level'} . '</i>');
            }
            if (!$rec->{'scoped_review'}) {
                push (@reasons, 'it has the wrong review type: <i>' . $rec->{'review'} . '</i>');
            }
            if ($#reasons == 0) {
                $rec->{'scopeDetails'} .= $reasons[0] . '.';
            } elsif ($#reasons == 1) {
                $rec->{'scopeDetails'} .= $reasons[0] . ' and ' . $reasons[1] . '.';
            } else {
                $rec->{'scopeDetails'} .= $reasons[0] . ', ' . $reasons[1] . ' and ' . $reasons[2] . '.';
            }
        }
        if ($rec->{'screened'}) {
            $rec->{'screenDetails'} = 'Record was not screened out because it has a valid ISSN and the correct ARY';
        } else {
            $rec->{'screenDetails'} = 'Record is screened out because ';
            my @reasons = ();
            if (!$rec->{'screened_issn'}) {
                push (@reasons, 'it does not have a valid ISSN');
            }
            if (!$rec->{'screened_year'}) {
                push (@reasons, 'it has the wrong year: <i>' . $rec->{'year'} . '</i>');
            }
            if ($#reasons == 0) {
                $rec->{'screenDetails'} .= $reasons[0] . '.';
            } else {
                $rec->{'screenDetails'} .= $reasons[0] . ' and ' . $reasons[1] . '.';
            }
        } 
        if ($rec->{'class'}) {
            $rec->{'classDetails'} = $rec->{'class'} . ' (' . $rec->{'class_reasons'} . ')';
        } else {
            $rec->{'classDetails'} = 'Record was not classified because ';
            if ($rec->{'scoped'}) {
                if ($rec->{'screened'}) {
                    $rec->{'classDetails'} .= 'of an unknown reason.';
                } else {
                    $rec->{'classDetails'} .= 'it was screened out.';
                }
            } else {
                if ($rec->{'screened'}) {
                    $rec->{'classDetails'} .= 'it is out of scoped.';
                } else {
                    $rec->{'classDetails'} .= 'it is out of scoped and screened out.';
                }
            }
        }
        if ($rec->{'fulltext_link'}) {
            my $pdf;
            if ($rec->{'fulltext_pdf'}) {
                $pdf = 'PDF';
            } else {
                $pdf = 'non-PDF';
            }
            if ($rec->{'fulltext_link_oa'}) {
                $rec->{'fulltext'} = 'Record has an Open Access $pdf link.';
                if ($rec->{'fulltext_downloaded'}) {
                    $rec->{'fulltext'} .= ' Fulltext has been downloaded successfully';
                    if ($rec->{'fulltext_verified'}) {
                        $rec->{'fulltext'} .= ' and is verified.';
                    } else {
                        $rec->{'fulltext'} .= ' but did not verify.';
                    }
                } else {
                    $rec->{'fulltext'} .= ' Fulltext as not been successfully downloaded.';
                }
            } else {
                $rec->{'fulltext'} = 'Record only has a non Open Access $pdf link.';
            }
        } else {
            $rec->{'fulltext'} = 'Record does not have a fulltext link.';
        }
        if ($rec->{'issn'} eq $rec->{'doaj_issn'}) {
            $rec->{'issn'} .= ' (DOAJ)';
        }
        if ($rec->{'eissn'} eq $rec->{'doaj_issn'}) {
            $rec->{'eissn'} .= ' (DOAJ)';
        }
        $rec->{'oai_link'} = $self->oai_link ($rec->{'source'}, $rec->{'source_id'});
        $rec->{'mxd_link'} = '/oa-indicator/ws' . $ENV{'PATH_INFO'};
        $rec->{'mxd_link'} =~ s|/record.html/|/recordMXDraw/|;
        if ($rec->{'duplicates'}) {
            my $links = '';
            foreach my $id (@{$rec->{'duplicates'}}) {
                if ($links) {
                    $links .= ', ';
                }
                $links .= '<a href="' . $id . '">' . $id . '</a>';
            }
            $rec->{'duplicates'} = $links;
        }
        return ($self->scr_parse ($self->scr_file ('record'), $rec));
    }
    return ("fatal: HTML format unsupported for command: $self->{'comm'}");
}

sub scr_file
{
    my ($self, $name) = @_;

    if (!-e "/etc/oa-indicator/html/$name") {
        if (-e "/etc/oa-indicator/html/$name.html") {
            $name .= '.html';
        } else {
            die ("fatal: could not find screen template: $name, check /etc/oa-indicator/html/ for possible names\n");
        }
    }
    open (my $fin, "/etc/oa-indicator/html/$name");
    return (join ('', <$fin>));
}

sub scr_parse
{
    my ($self, $txt, $var) = @_;

    my $pat = '<oa:([0-9a-z_]+) *([^>]*?)(?:/>|>(.*?)</oa:\1>)';
    while ($txt =~ s|$pat|__func__|osi) {
        my ($func, $attr, $body) = ($1, $2, $3);
        my $n = $body =~ s/<oa:$func/<oa:$func/g;
        while ($n > 0) {
            if ($txt =~ s/__func__(.*?)<\/oa:$func>/__func__/os) {
                $body .= "<\/oa:$func>" . $1;
            }
            $n = $body =~ s/(<oa:$func)/$1/g;
            $n -= $body =~ s/(<\/oa:$func)/$1/g;
        }
        my $s = $self->scr_function ($var, $func, $attr, $body);
        $txt =~ s/__func__/$s/;
    }
    return ($self->scr_replace_var ($var, $txt));
}

sub scr_replace_var
{
    my ($self, $var, $txt) = @_;

    while ($txt =~ s/\${([_A-Za-z\.]+)}/__var__/) {
        my ($name, $type) = split (/\./, $1);
        if (!defined ($type)) {
            $type = 'htencode';
        }
        if ($type eq 'raw') {
            $txt =~ s/__var__/$var->{$name}/;
            next;
        }
        if ($type eq 'htencode') {
            $txt =~ s/__var__/$self->html_encode ($var->{$name})/e;
            next;
        }
        $txt =~ s/__var__/error: unknown variable type: '$type'/;
    }
    return ($txt);
}

sub scr_function
{
    my ($self, $var, $name, $attr, $body) = @_;

    my $args = {};
    $attr =~ s/[\n\r]+/ /go;
    $attr =~ s|(?<!\\)\\||g;
    while ($attr =~ s/^ *([A-Za-z][0-9A-Za-z_:]+) *= *(["'])([^\2]*?)\2//) {
        $args->{$1} = $3;
        warn ("adding $1 arg with value $3\n");
    }
    if ($body) {
        $body =~ s/^[\s\n\r]+//;
        $body =~ s/[\s\n\r]+$//;
        if ($body) {
            $args->{'body'} = $body;
        }
    }
    $name = "func_$name";
    no strict 'refs';
    return (&{$name} ($self, $args, $var));
}

sub func_field
{
    my ($self, $args, $var) = @_;

    if (!exists ($args->{'name'})) {
        return ("Error: call to 'field' function missing 'name' attribute.");
    }
    if (!exists ($var->{$args->{'name'}})) {
        return ("Warning: unknown variable '$args->{'name'}' in function field.");
    }
    if ($var->{$args->{'name'}}) {
        if (exists ($args->{'label'})) {
            return ($args->{'label'} . $var->{$args->{'name'}});
        } else {
            return ($var->{$args->{'name'}});
        }
    }
}

sub func_if_var
{
    my ($self, $args, $var) = @_;

    if (!exists ($args->{'name'})) {
        return ("Error: call to 'if_var' function missing 'name' attribute.");
    }
    if (!exists ($var->{$args->{'name'}})) {
        return ("Warning: unknown variable '$args->{'name'}' in function if_var.");
    }
    if ($var->{$args->{'name'}}) {
        if ($args->{'not'}) {
            return ('');
        } else {
            return ($args->{'body'});
        }
    } else {
        if ($args->{'not'}) {
            return ($args->{'body'});
        } else {
            return ('');
        }
    }
}

sub html_encode
{
    my ($self, $txt) = @_;

    $txt =~ s/&/&amp;/g;
    $txt =~ s/</&lt;/g;
    $txt =~ s/>/&gt;/g;
    $txt =~ s/'/&apos;/g;
    $txt =~ s/"/&quot;/g;
    return ($txt);
}

sub respond_xml_encode
{
    my ($self, $parent, $data) = @_;

    my $xml = '';
    if (ref ($data) eq 'HASH') {
        foreach my $key (sort (keys (%{$data}))) {
            my $fld;
            my $attr = {};
            if ($key =~ m/^[0-9]+$/) {
                if ($parent =~ m/s$/) {
                    $fld = $parent;
                    $fld =~ s/s$//;
                } else {
                    $fld = $parent . '_child';
                }
                $attr->{'val'} = $key;
            } else {
                $fld = $key;
            }
            if (ref ($data->{$key}) eq 'ARRAY') {
                foreach my $d (@{$data->{$key}}) {
                    $xml .= $self->xml_start ($fld, $attr) . $self->respond_xml_encode ($fld, $d) . $self->xml_end ($fld);
                }
            } else {
                if ($fld eq 'mxd') {
                    $xml .= $self->xml_start ($fld, $attr) . $data->{$key} . $self->xml_end ($fld);
                } else {
                    $xml .= $self->xml_start ($fld, $attr) . $self->respond_xml_encode ($fld, $data->{$key}) . $self->xml_end ($fld);
                }
            }
        }
    } elsif (ref ($data) eq 'ARRAY') {
        foreach my $d (@{$data}) {
            $xml .= $self->xml_start ('array') . $self->respond_xml_encode ($parent, $d) . $self->xml_end ('array');
        }
    } else {
        $xml .= $self->xml_enc ($data);
    }
    return ($xml);
}

sub xml_start
{
    my ($self, $tag, $attr) = @_;
    my $xml = '<' . $tag;

    foreach my $a (sort (keys (%{$attr}))) {
        $xml .= ' ' . $a . '="' . $self->xml_enc ($attr->{$a}) . '"';
    }

    return ($xml . '>');
}

sub xml_end
{
    my ($self, $tag) = @_;

    return ('</' . $tag . '>');
}

sub xml_enc
{
    my ($self, $txt) = @_;

    if (defined ($txt)) {
        $txt =~ s/&/&amp;/g;
        $txt =~ s/</&lt;/g;
        $txt =~ s/>/&gt;/g;
        $txt =~ s/"/&quot;/g;
        $txt =~ s/'/&apos;/g;
    } else {
        $txt = '';
    }
    return ($txt);
}

sub date
{
    my ($self, $time) = @_;
    my ($sec, $min, $hour, $day, $mon, $year) = localtime ($time);
    
    return (sprintf ("%04d-%02d-%02d %02d:%02d:%02d", 1900 + $year, $mon + 1, $day, $hour, $min, $sec));
}

sub display_issn
{
    my ($self, $issn) = @_;

    my $ISSN = uc ($issn);
    $ISSN =~ s/[^0-9X]//g;
    if ($ISSN =~ m/^[0-9]{7}[0-9Xx]/) {
        return (substr ($ISSN, 0, 4) . '-' . substr ($ISSN, 4));
    } else {
        if ($ISSN =~ m/[0-9X]/) {
            return ($issn);
        } else {
            return ('');
        }
    }
}

sub display_issn_list
{
    my ($self, $list, $sep, $del) = @_;

    if (!$sep) {
        $sep = ',';
    }
    if (!$del) {
        $del = ',';
    }
    my @issn;
    foreach my $issn (split ($del, $list)) {
        my $i = $self->display_issn ($issn);
        if ($i) {
            push (@issn, $i);
        }
    }
    return (join ($sep, @issn));
}

sub error
{
    my ($self, $message) = @_;

    print (STDERR $message, "\n");
    $self->{'result'}{'response'}{'error'} = $message;
    $self->respond ();
}

1;

