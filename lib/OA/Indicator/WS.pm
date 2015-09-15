package OA::Indicator::WS;

use strict;
use warnings;
use Time::HiRes qw(time);
use JSON::XS;
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
    $self->{'result'} = {request => {datestamp => $self->date ()}, response => {}};
    ($v, $self->{'comm'}, $self->{'year'}, $self->{'type'}, $self->{'run'}) = split ('/', $ENV{'PATH_INFO'});
    ($self->{'comm'}, $self->{'format'}) = split (/\./, $self->{'comm'});
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
    if ($self->{'format'} !~ m/^(html|json|xml)$/) {
        $self->{'format'} = 'xml';
        $self->error ('unsupported format: ' . $self->{'format'} . '. Supported formats are: json, xml and html');
    }
    $self->{'result'}{'request'}{'year'} = $self->{'year'};
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
    my $valid = 0;
    foreach my $v ($self->{'db'}->run_years ()) {
        if ($self->{'year'} == $v) {
            $valid = 1;
            last;
        }
    }
    if (!$valid) {
        $self->error ('no data found for year: ' . $self->{'year'});
    }
    $valid = 0;
    foreach my $v ($self->{'db'}->run_types ($self->{'year'})) {
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
        foreach my $v ($self->{'db'}->run_runs ($self->{'year'}, $self->{'type'})) {
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
        foreach my $v ($self->{'db'}->run_runs ($self->{'year'}, $self->{'type'})) {
            if ($v > $self->{'run'}) {
                $self->{'run'} = $v;
            }
        }
    }
    my $db = $self->{'db'}->reuse ($self->{'year'}, $self->{'type'}, $self->{'run'});
    if ($self->{'comm'} eq 'national') {
        $self->comm_national ($db);
    }
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

    my $rs = $db->select ('class,count(*) as c', 'records', "class!=''", 'group by class');
    my $rc;
    while ($rc = $db->next ($rs)) {
        $self->{'result'}{'response'}{'body'}{$rc->{'class'}} = $rc->{'c'};
    }
    $self->respond ();
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
    } else {
        print ("Content-Type: text/xml\n\n");
        $result = '<?xml version="1.0"?>' . "\n";
        $result .= '<oa_indicator>';
        $result .= $self->respond_xml_encode ($self->{'result'});
        $result .= '</oa_indicator>';
    }
    $result =~ s/responseDatestamp/$self->date ()/e;
    $result =~ s/responseElapse/time - $self->{'start'}/e;
    print ($result);
    exit (0);
}

sub respond_xml_encode
{
    my ($self, $data) = @_;

    my $xml = '';
    if (ref ($data) eq 'HASH') {
        foreach my $key (sort (keys (%{$data}))) {
            if (ref ($data->{$key}) eq 'ARRAY') {
                foreach my $d (@{$data->{$key}}) {
                    $xml .= $self->xml_start ($key) . $self->respond_xml_encode ($d) . $self->xml_end ($key);
                }
            } else {
                $xml .= $self->xml_start ($key) . $self->respond_xml_encode ($data->{$key}) . $self->xml_end ($key);
            }
        }
    } elsif (ref ($data) eq 'ARRAY') {
        foreach my $d (@{$data}) {
            $xml .= $self->xml_start ('array') . $self->respond_xml_encode ($d) . $self->xml_end ('array');
        }
    } else {
        $xml .= $self->xml_enc ($data);
    }
    return ($xml);
}

sub xml_start
{
    my ($self, $tag, %attr) = @_;
    my $xml = '<' . $tag;

    foreach my $a (sort (keys (%attr))) {
        $xml .= ' ' . $a . '="' . &xml_enc ($attr{$a}) . '"';
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

    $txt =~ s/&/&amp;/g;
    $txt =~ s/</&lt;/g;
    $txt =~ s/>/&gt;/g;
    $txt =~ s/"/&quot;/g;
    $txt =~ s/'/&apos;/g;
    return ($txt);
}

sub date
{
    my ($sec, $min, $hour, $day, $mon, $year) = localtime (time);
    
    return (sprintf ("%04d-%02d-%02d %02d:%02d:%02d", 1900 + $year, $mon + 1, $day, $hour, $min, $sec));
}

sub error
{
    my ($self, $message) = @_;

    print (STDERR $message, "\n");
    $self->{'result'}{'response'}{'error'} = $message;
    $self->respond ();
}

1;

