import psycopg2
from collections import namedtuple
import random
from multiprocessing import Process, Array
import time
import sys
from numpy import mean

import config as conf


country_list = ['Polska', 'Australia', 'Kanada', 'Nibylandia']
person_list = ['Wendy', 'Piotrus', 'Kapitan Hak']
candy_list = []


def get_candy_list():
	return [conf.present_prefix + str(nr) for nr in range(conf.diff_kinds_max)]


def gen_elem(lst):
	return lst[random.randint(0, len(lst) - 1)]


def get_diff_candies(diff_kinds):
	global candy_list
	shuffled_list = candy_list.copy()
	random.shuffle(shuffled_list)
	return shuffled_list[:diff_kinds]


def gen_candies(diff_kinds):
	global candy_list
	candies = get_diff_candies(diff_kinds)
	quantities = [random.randint(conf.one_kind_lower_limit, conf.one_kind_upper_limit) for i in range(diff_kinds)]
	return list(zip(candies, quantities))


def generate_single_letter():
	Letter = namedtuple('Letter', ['country', 'person', 'candies'])

	diff_kinds = random.randint(conf.diff_kinds_lower_limit, conf.diff_kinds_upper_limit)
	candies = gen_candies(diff_kinds)
	letter = Letter(gen_elem(country_list), gen_elem(person_list), candies)
	return letter


def generate_letters():
	return [generate_single_letter() for i in range(conf.letters_nr)]


#==================


def print_psycopg2_exception(err):
	# get details about the exception
	err_type, err_obj, traceback = sys.exc_info()

	# get the line number when exception occured
	line_num = traceback.tb_lineno

	# print the connect() error
	print("\npsycopg2 ERROR:", err, "on line number:", line_num)
	print("psycopg2 traceback:", traceback, "-- type:", err_type)

	# psycopg2 extensions.Diagnostics object attribute
	print("\nextensions.Diagnostics:", err.diag)

	# print the pgcode and pgerror exceptions
	print("pgerror:", err.pgerror)
	print("pgcode:", err.pgcode, "\n")


def insert_parcel(conn, cursor, letter):
	q = """INSERT INTO paczka (kraj, imie) VALUES('{}', '{}') RETURNING id"""\
		.format(letter.country, letter.person)
	cursor.execute(q)
	rows = cursor.fetchall()

	return rows[0][0]


def insert_candy_in_parcel(cursor, parcel_id, candy_name, quantity):
	try:
		q = """INSERT INTO slodycz_w_paczce (id_paczki, slodycz, ilosc) VALUES('{}', '{}', '{}')"""\
			.format(parcel_id, candy_name, quantity)
		cursor.execute(q)

	except Exception as err:
		return False

	return True


def try_exact_candy(candy, candy_quantity, cursor, parcel_id):
	do_rollback = False
	success = False
	try:
		q ="""SELECT ilosc_pozostalych FROM slodycz_w_magazynie WHERE nazwa = '{}'"""\
			.format(candy)
		cursor.execute(q)
		rows = cursor.fetchall()
	except Exception as err:
		do_rollback = True
		print_psycopg2_exception(err)
		return success, do_rollback

	if len(rows) <= 0:
		do_rollback = True
		return success, do_rollback

	candy_left = rows[0][0] - candy_quantity
	if candy_left >= 0:
		try:
			q = """UPDATE slodycz_w_magazynie 
				SET ilosc_pozostalych = ilosc_pozostalych - {} 
				WHERE nazwa = '{}'"""\
				.format(candy_quantity, candy)
			cursor.execute(q)
		except Exception as err:
			do_rollback = True
			print_psycopg2_exception(err)
			return success, do_rollback

		#is successful
		success = insert_candy_in_parcel(cursor, parcel_id, candy, candy_quantity)

	if not success:
		do_rollback = True
	return success, do_rollback


def try_similar_candy(candy, candy_quantity, cursor, parcel_id):
	q = """SELECT slodycz_2 FROM podobny_slodycz WHERE slodycz_1 = '{}' ORDER BY podobienstwo DESC"""\
		.format(candy)
	cursor.execute(q)
	rows = cursor.fetchall()

	do_rollback = False
	success = False

	for row in rows:
		similar_candy = row[0]
		q = """SELECT ilosc_pozostalych FROM slodycz_w_magazynie WHERE nazwa = '{}'"""\
			.format(similar_candy)

		similar_candy_q_rows = cursor.execute(q)
		if len(similar_candy_q_rows) <= 0:
			continue

		# not enough candy in store
		if candy_quantity > similar_candy_q_rows[0][0]:
			continue

		try:
			q = """UPDATE slodycz_w_magazynie 
				SET ilosc_pozostalych = ilosc_pozostalych - {} 
				WHERE nazwa = '{}'"""\
				.format(candy_quantity, similar_candy)

			cursor.execute(q)

		except Exception as err:
			print_psycopg2_exception(err)
			do_rollback = True
			break

		# we took similar candy successfully
		success = True
		break

	if not do_rollback and success:
		success = insert_candy_in_parcel(cursor, parcel_id, candy, candy_quantity)

	if not success:
		do_rollback = True

	return success, do_rollback


def do_single_letter(conn, cursor, letter, lazy_worker):
	parcel_id = insert_parcel(conn, cursor, letter)

	success = False
	do_rollback = True

	for candy, candy_quantity in letter.candies:
		success, do_rollback = try_exact_candy(candy, candy_quantity, cursor, parcel_id)
		if do_rollback:
			break

		if success:
			continue

		success, do_rollback = try_similar_candy(candy, candy_quantity, cursor, parcel_id)

		if do_rollback:
			break

		if success:
			continue

	#worst place to be lazy
	if lazy_worker:
		time.sleep(conf.lazy_sleep_in_sec)

	# the letter was a success
	if do_rollback:
		conn.rollback()
		return 0
	else:
		try:
			conn.commit()
			return 1
		except:
			return 0


def elf_work(letters, time_arr, success_arr, id, lazy_worker):
	successful_trans = 0
	time_sum = 0

	for letter in letters:
		conn = psycopg2.connect(user='vaen')
		conn.set_isolation_level(conf.isolation_lvl)
		cursor = conn.cursor()

		start_time = time.perf_counter()

		successful_trans += do_single_letter(conn, cursor, letter, lazy_worker)
		end_time = time.perf_counter()

		time_sum += end_time - start_time

		#write to shared arrays
		time_arr[id] = time_sum
		success_arr[id] = successful_trans

		conn.close()
	print("""letter/second: {}""".format(len(letters) / time_sum))
	print("""successful trans: {}/{}""".format(successful_trans, len(letters)))

	return


def main():
	global candy_list
	candy_list = get_candy_list()
	nr_of_elves = 18
	nr_of_lazy = 2
	nr_of_all_workers = nr_of_elves + nr_of_lazy
	letters_list = [generate_letters() for i in range(nr_of_all_workers)]
	nr_of_trans = sum([len(letters) for letters in letters_list]) * nr_of_elves / 20

	time_arr = Array('d', [0] * (nr_of_all_workers))
	success_arr = Array('i', [0] * (nr_of_all_workers))

	elves = [Process(target=elf_work, args=(letters_list[id], time_arr, success_arr, id, False))
				for id in range(nr_of_elves)]

	lazy_workers = [Process(target=elf_work, args=(letters_list[id], time_arr, success_arr, id, True))
				   for id in range(nr_of_elves, nr_of_all_workers)]

	#start working
	for lazy_one in lazy_workers:
		lazy_one.start()

	for elf in elves:
		elf.start()

	seconds = 10
	time.sleep(seconds)

	for elf in elves:
		elf.terminate()

	for lazy_one in lazy_workers:
		lazy_one.terminate()


	# cut off lazy results
	if nr_of_lazy > 0:
		success_arr = success_arr[:-nr_of_lazy]
		time_arr = time_arr[:-nr_of_lazy]

	successful_trans_per_sec = []
	for a, b in zip(success_arr, time_arr):
		successful_trans_per_sec.append(a/b)

	avg_successful_trans_per_sec = mean(successful_trans_per_sec)
	print("""avg_successful_trans_per_sec: {:.2f}""".format(avg_successful_trans_per_sec))

	success_percent = 100 * sum(success_arr)/nr_of_trans
	print("""successful transactions: {}/{}, {:.2f}%""".format(sum(success_arr), nr_of_trans, success_percent))



main()
