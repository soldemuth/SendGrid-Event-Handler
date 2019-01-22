package SendGridTable;

use strict;
use warnings;

use DBI;

$SendGridTable::_dbh = undef;

sub dbh {
    my ($class) = @_;

    unless (
           ref($SendGridTable::_dbh) eq 'DBI'
        && $SendGridTable::_dbh->ping()
    ) {
        $SendGridTable::_dbh =
            # update for FACT!
            DBI->connect(
                  'DBI:mysql:...;'
                . 'mysql_init_command=SET time_zone="America/Chicago"',
                '...',
                '...',
                {
                    AutoCommit => 1,
                    RaiseError => 1,
                    PrintError => 0,
                },
            );
    }

    return $SendGridTable::_dbh;
}

sub disconnect {
    my ($class) = @_;

    if (ref($SendGridTable::_dbh) eq 'DBI') {
        $SendGridTable::_dbh->disconnect();

        $SendGridTable::_dbh = undef;
    }
}

1;
