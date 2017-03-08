package OA::Indicator::XML;

use strict;
use warnings;
use XML::LibXML;

our $VERSION = '1.0';

sub new
{
    my ($class, %prefix) = @_;
    my $self = {};

    if (%prefix) {
        $self->{'prefix'} = \%prefix;
    }
    return (bless ($self, $class));
}

sub parse
{
    my ($self, $xml) = @_;
    my ($doc, $xpc);

    if (!$self->{'parser'}) {
        $self->{'parser'} = new XML::LibXML ();
    }
    eval {
        $doc = $self->{'parser'}->parse_string ($xml);
    };
    if ($@) {
        die ("XML parse error: $@\nXML: $xml\n - ");
    }
    if ($self->{'prefix'}) {
        $xpc = new XML::LibXML::XPathContext ($doc);
        foreach my $pre (keys (%{$self->{'prefix'}})) {
            if ($self->{'prefix'}{$pre} eq 'mxdns') {
                my $uri;
                if ($xml =~ m|http://mx.forskningsdatabasen.dk/ns/documents/1.3|) {
                    $uri = 'http://mx.forskningsdatabasen.dk/ns/documents/1.3';
                } elsif ($xml =~ m|http://mx.forskningsdatabasen.dk/ns/documents/1.4|) {
                    $uri = 'http://mx.forskningsdatabasen.dk/ns/documents/1.4';
                } else {
                    die ("could not find version of MXD for: '$xml'");
                }
                $xpc->registerNs ($pre, $uri);
            } else {
                $xpc->registerNs ($pre, $self->{'prefix'}{$pre});
            }
        }
        $self->{'doc'} = $xpc;
        return ($xpc);
    } else {
        $self->{'doc'} = $doc;
        return ($doc);
    }
}

sub node
{
    my ($self, $xpath, $doc) = @_;
    my (@ret, $node);

    if (!defined ($doc)) {
        $doc = $self->{'doc'};
    }
    foreach $node ($doc->findnodes ($xpath)) {
        if (wantarray) {
            push (@ret, $node);
        } else {
            return ($node);
        }
    }
    if (wantarray) {
        return (@ret);
    } else {
        return (undef);
    }
}

sub field
{
    my ($self, $xpath, $doc) = @_;
    my (@ret, $node, $val);

    if (!defined ($doc)) {
        $doc = $self->{'doc'};
    }
    foreach $node ($doc->findnodes ($xpath)) {
        $node = $node->firstChild;
        if (ref ($node) eq 'XML::LibXML::Text') {
            $val = $node->to_literal ();
            if (wantarray) {
                push (@ret, $val);
            } else {
                return ($val);
            }
        }
    }
    if (wantarray) {
        return (@ret);
    } else {
        return ('');
    }
}

1;

