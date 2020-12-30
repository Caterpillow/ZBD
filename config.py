import psycopg2

# ile najwiecej roznych slodyczy moze byc w 1 liscie
diff_kinds_upper_limit = 4
diff_kinds_lower_limit = 1
# diff_kinds_upper_limit = 5
# diff_kinds_lower_limit = 5

diff_kinds_max = 30
# diff_kinds_max = 4000


# jak duzo wierszy w tabeli podobnych slodyczy
similar_candy = 3 * diff_kinds_max
present_prefix = 'present'

# ile najwiecej moze byc tego samego slodycza w 1 liscie
one_kind_upper_limit = 2
one_kind_lower_limit = 1
# one_kind_upper_limit = 4
# one_kind_lower_limit = 4
one_kind_mx = 300
# one_kind_mx = 50
# one_kind_mx = 20000


letters_nr = 100

# isolation_lvl = psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
# isolation_lvl = psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ
isolation_lvl = psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
# isolation_lvl = psycopg2.extensions.ISOLATION_LEVEL_READ_UNCOMMITTED

lazy_sleep_in_sec = 1
