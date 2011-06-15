import cql
from django.core.management.base import NoArgsCommand

class Command(NoArgsCommand):
    def handle_noargs(self, **options):
        conn = cql.connect('localhost', 9160)
        cursor = conn.cursor()

        for ks_def in conn.client.describe_keyspaces():
            if ks_def.name == 'twissandra':
                msg = 'Looks like you already have a Twissandra keyspace.\nDo you '
                msg += 'want to delete it and recreate it? All current data will '
                msg += 'be deleted! (y/n): '
                resp = raw_input(msg)
                if not resp or resp[0] != 'y':
                    print "Ok, then we're done here."
                    return
                cursor.execute("DROP KEYSPACE twissandra")

        cursor.execute("CREATE KEYSPACE twissandra WITH strategy_class='SimpleStrategy' and strategy_options:replication_factor=1")
        cursor.execute("USE twissandra")
        cursor.execute("CREATE COLUMNFAMILY users (key varchar PRIMARY KEY, password varchar)")
        cursor.execute("CREATE COLUMNFAMILY following (key uuid PRIMARY KEY, followed varchar, followed_by varchar)")
        cursor.execute("CREATE INDEX following_followed ON following(followed)")
        cursor.execute("CREATE INDEX following_followed_by ON following(followed_by)")
        # TODO make tweet key uuid
        cursor.execute("CREATE COLUMNFAMILY tweets (key varchar PRIMARY KEY, user_id varchar, body varchar)")
        cursor.execute("CREATE COLUMNFAMILY timeline (key varchar PRIMARY KEY) WITH comparator=uuid")
        cursor.execute("CREATE COLUMNFAMILY userline (key varchar PRIMARY KEY) WITH comparator=uuid")

        print 'All done!'
