package OA::Indicator::MXD;

use strict;
use warnings;
use OA::Indicator::XML;

sub new
{
    my ($class) = @_;
    my $self = {};

    $self->{'whitelist'} = [];
    open (my $fh, '/etc/oa-indicator/whitelist.tab');
    while (<$fh>) {
        if (m/^#/) {
            next;
        }
        chomp;
        my ($name, $shortname, $domain) = split ("\t", $_);
        push (@{$self->{'whitelist'}}, $domain);
    }
    close ($fh);
    $self->{'primary_fields'} = [qw(source_id year type level review research_area jno pno jtitle jtitle_alt doi title_main title_sub pubyear)];
    $self->{'xpath'} = {
        source_id            => '/mxd:ddf_doc/@rec_id',
        year                 => '/mxd:ddf_doc/@doc_year',
        type                 => '/mxd:ddf_doc/@doc_type',
        level                => '/mxd:ddf_doc/@doc_level',
        review               => '/mxd:ddf_doc/@doc_review',
        research_area        => '/mxd:ddf_doc/mxd:description/mxd:research_area/@area_code',
        jno                  => '/mxd:ddf_doc/mxd:publication/@bfi_serial_no',
        pno                  => '/mxd:ddf_doc/mxd:publication/@bfi_publisher_no',
        jtitle               => '/mxd:ddf_doc/mxd:publication/mxd:in_journal/mxd:title',
        jtitle_alt           => '/mxd:ddf_doc/mxd:publication/mxd:in_journal/mxd:title_alternative',
        doi                  => '//mxd:doi',
        title_main           => '/mxd:ddf_doc/mxd:title/mxd:original/mxd:main',
        title_sub            => '/mxd:ddf_doc/mxd:title/mxd:original/mxd:sub',
        pubyear              => '/mxd:ddf_doc/mxd:publication/*/mxd:year',
        do                   => '/mxd:ddf_doc/mxd:publication/mxd:digital_object',
        do_role              => '@role',
        do_access            => '@access',
        do_size              => 'mxd:file/@size',
        do_mime              => 'mxd:file/@mime_type',
        do_file              => 'mxd:file/@filename',
        do_uri               => 'mxd:uri',
        inet                 => '/mxd:ddf_doc/mxd:publication/mxd:inetpub',
        inet_text            => 'mxd:text',
        inet_uri             => 'mxd:uri',
        inet_access          => 'mxd:uri/@access',
        pissn                => '//mxd:issn[@type="pri"]',
        eissn                => '//mxd:issn[@type="ele"]',
        person               => '/mxd:ddf_doc/mxd:person',
        person_role          => '@pers_role',
        person_first         => 'mxd:name/mxd:first',
        person_last          => 'mxd:name/mxd:last',
        person_id            => 'mxd:id',
        person_id_type       => '@id_type',
        person_id_source     => '@id_source',
        person_id_id         => '.',
        person_email         => 'mxd:email',
    };
    $self->{'xml'} = new OA::Indicator::XML (mxd => 'http://mx.forskningsdatabasen.dk/ns/documents/1.3');
    return (bless ($self, $class));
}

sub parse
{
    my ($self, $xml) = @_;

    $self->{'doc'} = $self->{'xml'}->parse ($xml);
    $self->{'rec'} = {};
    foreach my $f (qw(source_id)) {
        $self->{'rec'}{$f} = $self->{'xml'}->field ($self->{'xpath'}{$f});
    }
}

sub id
{
    my ($self) = @_;

    return ($self->{'rec'}{'source_id'});
}

sub primary_fields
{
    my ($self) = @_;

    return (@{$self->{'primary_fields'}});
}

sub primary
{
    my ($self) = @_;
    my @ret = ();
    foreach my $f (@{$self->{'primary_fields'}}) {
        if (!exists ($self->{'rec'}{$f})) {
            $self->{'rec'}{$f} = $self->{'xml'}->field ($self->{'xpath'}{$f});
        }
        push (@ret, $self->{'rec'}{$f});
    }
    return (@ret);
}

sub fulltext_fields
{
    return (qw(type uri access text role size mime filename));
}

sub fulltext
{
    my ($self) = @_;
    my @ret = ();

    foreach my $obj ($self->{'xml'}->node ($self->{'xpath'}{'do'})) {
        my $rec = ['digital_object'];
        foreach my $f (qw(do_uri do_access blank do_role do_size do_mime do_file)) {
            if ($f eq 'blank') {
                push (@{$rec}, '');
            } else {
                my $s;
                if ($s = $self->{'xml'}->field ($self->{'xpath'}{$f}, $obj)) {
                    push (@{$rec}, $s);
                } else {
                    push (@{$rec}, '');
                }
            }
        }
        push (@ret, $rec);
    }
    foreach my $obj ($self->{'xml'}->node ($self->{'xpath'}{'inet'})) {
        my $rec = ['inetpub'];
        foreach my $f (qw(inet_uri inet_access inet_text)) {
            my $s;
            if ($s = $self->{'xml'}->field ($self->{'xpath'}{$f}, $obj)) {
                push (@{$rec}, $s);
            } else {
                push (@{$rec}, '');
            }
        }
        my $url = $rec->[1];
        if ($url =~ m/\/\/([^\/]+)/) {
            my $host = $1;
            my $white = 0;
            foreach my $dom (@{$self->{'whitelist'}}) {
                if (($host eq $dom) || ($host =~ m/\.$dom$/)) {
                    $white = 1;
                    last;
                }
            }
            if (!$white) {
                next;
            }
        } else {
            warn ("could not extra hostname from URL: $url (" . $self->id () . "\n");
            next;
        }
        push (@ret, $rec);
    }
    return (@ret);
}

sub issn_fields
{
    return ('type', 'issn');
}

sub issn
{
    my ($self) = @_;
    my @ret = ();

    foreach my $issn ($self->{'xml'}->field ($self->{'xpath'}{'pissn'})) {
        push (@ret, ['print', $issn]);
    }
    foreach my $issn ($self->{'xml'}->field ($self->{'xpath'}{'eissn'})) {
        push (@ret, ['electronic', $issn]);
    }
    return (@ret);
}

sub person_fields
{
    return (qw(pos role first last email id type source));
}

sub person
{
    my ($self) = @_;
    my @ret = ();

    my $n = 1;
    foreach my $obj ($self->{'xml'}->node ($self->{'xpath'}{'person'})) {
        my $rec = [$n++];
        foreach my $f (qw(person_role person_first person_last person_email person_id)) {
            if ($f eq 'person_id') {
                foreach my $id ($self->{'xml'}->node ($self->{'xpath'}{$f}, $obj)) {
                    foreach my $fld (qw(person_id_id person_id_type person_id_source)) {
                        my $s;
                        if ($s = $self->{'xml'}->field ($self->{'xpath'}{$fld}, $id)) {
                            push (@{$rec}, $s);
                        } else {
                            push (@{$rec}, '');
                        }
                    } 
                }
            } else {
                my $s;
                if ($s = $self->{'xml'}->field ($self->{'xpath'}{$f}, $obj)) {
                    push (@{$rec}, $s);
                } else {
                    push (@{$rec}, '');
                }
            }
        }
        push (@ret, $rec);
    }
    return (@ret);
}

1;

