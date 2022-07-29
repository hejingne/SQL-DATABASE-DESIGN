"""
# This code is provided solely for the personal and private use of students
# taking the CSC343H course at the University of Toronto. Copying for purposes
# other than this use is expressly prohibited. All forms of distribution of
# this code, including but not limited to public repositories on GitHub,
# GitLab, Bitbucket, or any other online platform, whether as given or with
# any changes, are expressly prohibited.
"""

from typing import Optional
import psycopg2 as pg
import datetime


class Assignment2:

    ##### DO NOT MODIFY THE CODE BELOW. #####

    def __init__(self) -> None:
        """Initialize this class, with no database connection yet.
        """
        self.db_conn = None

    def connect_db(self, url: str, username: str, pword: str) -> bool:
        """Connect to the database at url and for username, and set the
        search_path to "air_travel". Return True iff the connection was made
        successfully.

        >>> a2 = Assignment2()
        >>> # This example will make sense if you change the arguments as
        >>> # appropriate for you.
        >>> a2.connect_db("csc343h-<your_username>", "<your_username>", "")
        True
        >>> a2.connect_db("test", "postgres", "password") # test doesn't exist
        False
        """
        try:
            self.db_conn = pg.connect(dbname=url, user=username, password=pword,
                                      options="-c search_path=air_travel")
        except pg.Error:
            return False

        return True

    def disconnect_db(self) -> bool:
        """Return True iff the connection to the database was closed
        successfully.

        >>> a2 = Assignment2()
        >>> # This example will make sense if you change the arguments as
        >>> # appropriate for you.
        >>> a2.connect_db("csc343h-<your_username>", "<your_username>", "")
        True
        >>> a2.disconnect_db()
        True
        """
        try:
            self.db_conn.close()
        except pg.Error:
            return False

        return True

    ##### DO NOT MODIFY THE CODE ABOVE. #####

    # ----------------------- Airline-related methods ------------------------- */

    def book_seat(self, pass_id: int, flight_id: int, seat_class: str) -> Optional[bool]:
        """Attempts to book a flight for a passenger in a particular seat class.
        Does so by inserting a row into the Booking table.

        Read the handout for information on how seats are booked.

        Parameters:
        * pass_id - id of the passenger
        * flight_id - id of the flight
        * seat_class - the class of the seat

        Precondition:
        * seat_class is one of "economy", "business", or "first".

        Return:
        * True iff the booking was successful.
        * False iff the seat can't be booked, or if the passenger or flight cannot be found.
        """
        try:
            # 1. Open a cursor object
            cur = self.db_conn.cursor()

            # 2.a. Return False if the passenger can't be found
            cur.execute("SELECT * "
                        "FROM Passenger "
                        "WHERE id = %s;", (pass_id,))
            found_psg = cur.fetchone()  # returns a single tuple
            if found_psg is None:
                return False

            # 2.b. Return False if the flight can't be found
            cur.execute("SELECT * "
                        "FROM Flight "
                        "WHERE id = %s;", (flight_id,))
            found_flight = cur.fetchone()
            if found_flight is None:
                return False

            # 2.c. Return False if the seat can't be booked
            #   c.1. Check the number of booked seats for this seat class of
            #   this flight
            cur.execute("SELECT count(id) "
                        "FROM Booking "
                        "WHERE flight_id = %s AND seat_class = %s "
                        "GROUP BY flight_id, seat_class;",
                        (flight_id, seat_class))
            count = cur.fetchone()
            booked_capacity = 0 if count is None else count[0]

            #   c.2. Check the capacity for this seat class of this flight
            cur.execute("SELECT capacity_economy, "
                        "       capacity_business, "
                        "       capacity_first "
                        "FROM Flight JOIN Plane ON ("
                        "     plane = tail_number AND "
                        "     Flight.airline = Plane.airline) "
                        "WHERE id = %s;", (flight_id,))
            all_capacities = cur.fetchone()
            capacities_dict = {"economy": all_capacities[0],
                               "business": all_capacities[1],
                               "first": all_capacities[2]}
            capacity = capacities_dict.get(seat_class)

            #   c.3. Return False if 1. seat_class == 'first' or 'business'
            #          and no more seat can be booked
            #          or 2. seat_class == 'economy'
            #          and 10 extra seats have been booked
            # p.s. used ">=" operator just in case somehow there are
            # invalid overbooked seats in the table when it is first imported
            if seat_class != "economy" and booked_capacity >= capacity:
                return False
            if seat_class == "economy" and booked_capacity - capacity >= 10:
                return False

            # 3. Book seat
            #   Compute the text representation of seat to insert into table
            #   or NULL for valid overbooked economy seat
            seat_to_book = booked_capacity + 1
            seat_is_null = (seat_class == "economy" and
                            seat_to_book >= capacity and
                            seat_to_book - capacity <= 10)

            if seat_is_null:
                cur.execute("INSERT INTO Booking VALUES "
                            "(%s, %s, %s, %s, %s, %s, NULL, NULL);",
                            (self._generate_new_booking_id(cur),
                             pass_id,
                             flight_id,
                             self._get_current_timestamp(),
                             self._get_booking_price(cur,
                                                     seat_class,
                                                     flight_id),
                             "economy"
                             ))
            else:
                cur.execute("INSERT INTO Booking VALUES "
                            "(%s, %s, %s, %s, %s, %s, %s, %s);",
                            (self._generate_new_booking_id(cur),
                             pass_id,
                             flight_id,
                             self._get_current_timestamp(),
                             self._get_booking_price(cur,
                                                     seat_class,
                                                     flight_id),
                             seat_class,
                             self._compute_seat_row(capacities_dict,
                                                    seat_class,
                                                    seat_to_book),
                             self._compute_seat_letter(seat_class,
                                                       seat_to_book)
                             ))
            self.db_conn.commit()
            cur.close()
            return True

        except pg.Error:
            return None

    def upgrade(self, flight_id: int) -> Optional[int]:
        """Attempts to upgrade overbooked economy passengers to business class
        or first class (in that order until each seat class is filled).
        Does so by altering the database records for the bookings such that the
        seat and seat_class are updated if an upgrade can be processed.

        Upgrades should happen in order of earliest booking timestamp first.
        If economy passengers are left over without a seat (i.e. not enough higher class seats),
        remove their bookings from the database.

        Parameters:
        * flight_id - the flight to upgrade passengers in

        Precondition:
        * flight_id exists in the database (a valid flight id).

        Return:
        * The number of passengers upgraded.
        """
        try:
            # 1. Open a cursor object
            cur = self.db_conn.cursor()

            # 2. Check how many seats from first & business class are available
            #    to assign for economy overbooked seats

            #   2.1. Get the total capacities of first and business class
            cur.execute("SELECT capacity_economy, "
                        "       capacity_business, "
                        "       capacity_first "
                        "FROM Flight JOIN Plane ON ("
                        "     plane = tail_number AND "
                        "     Flight.airline = Plane.airline) "
                        "WHERE id = %s;", (flight_id,))
            all_capacities = cur.fetchone()
            capacities_dict = {"economy": all_capacities[0],
                               "business": all_capacities[1],
                               "first": all_capacities[2]}
            business_capacity = all_capacities[1]
            first_capacity = all_capacities[2]

            #   2.2. Get the number of booked seats for first and business class
            cur.execute("SELECT count(id) "
                        "FROM Booking "
                        "WHERE flight_id = %s AND seat_class = 'business' "
                        "GROUP BY flight_id;",
                        (flight_id,))
            count = cur.fetchone()
            business_booked = 0 if count is None else count[0]
            cur.execute("SELECT count(id) "
                        "FROM Booking "
                        "WHERE flight_id = %s AND seat_class = 'first' "
                        "GROUP BY flight_id;",
                        (flight_id,))
            count = cur.fetchone()
            first_booked = 0 if count is None else count[0]

            #   2.3. Subtract to get the number of available first and
            #   business class seats
            avail_business = business_capacity - business_booked
            avail_first = first_capacity - first_booked

            # 3. Sort and assign seats for overbooked economy seats until
            # all available first and business class seats are used up
            upgraded = 0

            #   3.1. Find all bookings that need upgrade if there are available
            #   business class seats
            if avail_business != 0:
                cur.execute("SELECT id "
                            "FROM Booking "
                            "WHERE flight_id = %s AND "
                            "      seat_class = 'economy' AND "
                            "      row IS NULL AND "
                            "      letter IS NULL "
                            "ORDER BY datetime;", (flight_id,))
                rows = cur.fetchall()

                #   3.2. Modify Booking table to upgrade
                for row in rows:
                    if avail_business != 0:
                        seat_class = "business"
                        seat_to_book = business_booked + 1
                        id = row[0]
                        cur.execute("UPDATE Booking "
                                    "SET seat_class = 'business', "
                                    "    row = %s, "
                                    "    letter = %s "
                                    "WHERE id = %s;",
                                    (self._compute_seat_row(capacities_dict,
                                                            seat_class,
                                                            seat_to_book),
                                    self._compute_seat_letter(seat_class,
                                                              seat_to_book),
                                    id))
                        business_booked += 1
                        avail_business -= 1
                        upgraded += 1
                    else:
                        break

            # 4. Repeat the steps for first class
            #   4.1. Find all bookings that need upgrade if there are available
            #   first class seats
            if avail_first != 0:
                cur.execute("SELECT id "
                            "FROM Booking "
                            "WHERE seat_class = 'economy' AND "
                            "      row IS NULL AND "
                            "      letter IS NULL "
                            "ORDER BY datetime;", (flight_id,))
                rows = cur.fetchall()

                #   4.2. Modify Booking table to upgrade
                for row in rows:
                    if avail_first != 0:
                        seat_class = "first"
                        seat_to_book = first_booked + 1
                        id = row[0]
                        cur.execute("UPDATE Booking "
                                    "SET seat_class = 'first', "
                                    "    row = %s, "
                                    "    letter = %s "
                                    "WHERE id = %s;",
                                    (self._compute_seat_row(capacities_dict,
                                                            seat_class,
                                                            seat_to_book),
                                    self._compute_seat_letter(seat_class,
                                                              seat_to_book),
                                    id))
                        first_booked += 1
                        avail_first -= 1
                        upgraded += 1
                    else:
                        break

            # 5. Remove overbooked economy seats when there are no seats
            #    remaining in business/first class
            cur.execute("DELETE FROM Booking "    
                        "WHERE flight_id = %s AND seat_class = 'economy' AND "
                        "      row IS NULL AND "
                        "      letter IS NULL;", (flight_id,))
            self.db_conn.commit()
            cur.close()
            return upgraded
        except pg.Error:
            return None

    # ----------------------- Helper methods below  ------------------------- */

    # A helpful method for adding a timestamp to new bookings.
    def _get_current_timestamp(self):
        """Return a datetime object of the current time, formatted as required
        in our database.
        """
        return datetime.datetime.now().replace(microsecond=0)

    ## Add more helper methods below if desired.
    def _compute_seat_row(self, capacities_dict: dict, seat_class: str,
                          seat_to_book: int):
        """Return an integer value for the seat row used for a successful
        booking. The function book_seat() ensures the seat about to be booked
        is valid (legally (over)booked) when this function is called

        Parameters:
        * capacities_dict - a dictionary with seat class string as key
                            and the flight's capacity for such seat class
                            as value
        * seat_class - the class of the seat
        * seat_to_book - an integer value representing the nth seat to be booked
          for this class. i.e. When booking a seat for a passenger who is the
          8th passenger to book the class, seat_to_book equals to 8

        Return:
        * an integer value for the seat row used for a successful booking.
        """
        # variable starting_row_minus_1 denotes the row before the staring row
        # of a class
        if seat_class == "first":
            starting_row_minus_1 = 0
        elif seat_class == "business":
            first_class_capacity = capacities_dict.get("first")
            starting_row_minus_1 = (first_class_capacity // 6) + 1 \
                if first_class_capacity % 6 != 0 \
                else first_class_capacity // 6
        else:
            first_class_capacity = capacities_dict.get("first")
            starting_row_minus_1_business = (first_class_capacity // 6) + 1 \
                if first_class_capacity % 6 != 0 \
                else first_class_capacity // 6

            business_class_capacity = capacities_dict.get("business")
            starting_row_minus_1_eco = (business_class_capacity // 6) + 1 \
                if business_class_capacity % 6 != 0 \
                else business_class_capacity // 6

            starting_row_minus_1 = starting_row_minus_1_business + \
                                   starting_row_minus_1_eco

        row = (seat_to_book // 6) + 1 \
            if seat_to_book % 6 != 0 \
            else seat_to_book // 6
        seat_row = row + starting_row_minus_1

        return seat_row

    def _compute_seat_letter(self, seat_class: str, seat_to_book: int):
        """Return an integer value for the seat letter used for a successful
        booking.

        Parameters:
        * seat_class - the class of the seat
        * seat_to_book - an integer value representing the nth seat to be booked
          for this class. i.e. When booking a seat for a passenger who is the
          8th passenger to book the class, seat_to_book equals to 8

        Return:
        * an integer value for the seat letter used for a successful booking.
        """
        idx_alphabet_dict = {1: "A", 2: "B", 3: "C", 4: "D", 5: "E", 6: "F"}

        seat_idx = seat_to_book % 6 \
            if seat_to_book % 6 != 0 \
            else 6
        seat_letter = idx_alphabet_dict.get(seat_idx)

        return seat_letter

    def _get_booking_price(self, cur, seat_class: str, flight_id: int):
        """Return the booking price for specified seat class. The function
        book_seat() ensures the flight_id is valid before this function
        is called
        Parameters:
        * cur - A cursor object that has been opened
        * seat_class - the class of the seat
        * flight_id - flight id for the booking

        Return:
        * an integer value for the booking price for specified seat class.
        """
        cur.execute("SELECT economy, business, first "
                    "FROM Price "
                    "WHERE flight_id = %s;", (flight_id,))
        class_col_idx_dict = {"economy": 0, "business": 1, "first": 2}
        return cur.fetchone()[class_col_idx_dict.get(seat_class)]

    def _generate_new_booking_id(self, cur):
        """Return a new booking id computed by adding 1 to the max(id) present
        in the Booking table

        Parameters:
        * cur - A cursor object that has been opened

        Return:
        * an integer value for a new booking id computed by adding 1 to
        max(id) in the Booking table.
        """
        cur.execute("SELECT max(id) + 1"
                    "FROM Booking;")
        return cur.fetchone()[0]
        
# ----------------------- Testing code below  ------------------------- */

def sample_testing_function() -> None:
    a2 = Assignment2()
    print("Connected to db -- " + str(a2.connect_db("csc343h-hejingne", "hejingne", "")))

    # Test booking for passenger that does not exist
    print("Book for invalid passenger -- "+
          str(a2.book_seat(100, 1, "first")) + "\n")  # should be False

    # Test booking for flight that does not exist
    print("Book for invalid flight -- " +
          str(a2.book_seat(1, 100, "first")) + "\n")  # should be False

    # Test successfully booking for first class
    print("Legitimately book for first class -- " +
          str(a2.book_seat(1, 1, "first")) + "\n")  # should be True

    # Test trying to overbook first class
    print("Overbook for first class -- " +
          str(a2.book_seat(1, 5, "first")) + "\n")  # should be False

    # Test successfully booking for business class
    #   (flight 5 (TSTST) is now fully booked for business class)
    print("Legitimately book for business class -- " +
          str(a2.book_seat(1, 5, "business")) + "\n")  # should be True

    # Test trying to overbook business class
    print("Overbook for business class -- " +
          str(a2.book_seat(1, 5, "business")) + "\n")  # should be False

    # Test successfully booking for economy class
    print("Legitimately book for economy class -- " +
          str(a2.book_seat(1, 1, "economy")) + "\n")  # should be True

    # Test successfully overbook economy class
    #   (flight 10 (ABCDE) now has 2 overbooked eco seat after this booking)
    print("Legitimately Overbook for economy class -- " +
          str(a2.book_seat(2, 10, "economy")) + "\n")  # should be True

def testing_function_1() -> None:
    a2 = Assignment2()
    print("Connected to db -- " + str(
        a2.connect_db("csc343h-hejingne", "hejingne", "")))

    # Test trying to overbook economy class when there are >= 10 overbooked
    #   economy seats
    #   (flight 10 (ABCDE) now has 10 overbooked eco seat after bookings below)
    for i in range(8):
        a2.book_seat(2, 10, "economy")
    print("Overbook for economy class -- " +
          str(a2.book_seat(2, 10, "economy")) + "\n")  # should be False

    # Test successfully upgrade economy seats and remove extra
    # overbooked economy seats
    #   (flight 10 (ABCDE) still has 1 business seat available before
    #   this upgrade)
    print("Legitimately upgrade economy seat(s) " + str(a2.upgrade(10)) + "\n")
    # should be 1

    # Test no higher class seat can be booked after the booking from above
    #   (flight 10 (ABCDE) still has 0 higher class seat available before
    #   this upgrade)
    print("Overbook higher class seat after eco seat upgrade -- " +
          str(a2.book_seat(3, 10, "business")) + "\n")  # should be False

    # Test economy seats can still be booked after the booking from above
    # but can not upgrade since no higher class seat left
    #   (flight 10 (ABCDE) still has 0 higher class seat available before
    #   this upgrade)
    print("Legitimately Overbook economy class seat after eco seat upgrade -- "
          + str(a2.book_seat(4, 10, "economy")) + "\n") # should be True

    print("Over upgrade economy seat(s) " + str(a2.upgrade(10)) + "\n")
    # should be 0

def testing_function_2() -> None:
    a2 = Assignment2()
    print("Connected to db -- " + str(
        a2.connect_db("csc343h-hejingne", "hejingne", "")))

    # Test successfully booking several first & business & economy seats
    # for a flight that does not have any booking yet to check if seats
    # are arranged correctly
    first_cnt = 0
    for i in range(7):
        if a2.book_seat(4, 9, "first"):
            first_cnt += 1
    print("Legitimately book for first class -- True " + "for " +
          str(first_cnt) + " times " + "\n")  # should be 7

    business_cnt = 0
    for i in range(7):
        if a2.book_seat(4, 9, "business"):
            business_cnt += 1
    print("Legitimately book for business class -- True " + "for " +
          str(business_cnt) + " times " + "\n")  # should be 7

    print("Legitimately book for economy class -- " +
          str(a2.book_seat(4, 9, "economy")) + "\n")  # should be True

    first_cnt = 0
    for i in range(2):
        if a2.book_seat(4, 9, "first"):
            first_cnt += 1
    print("Legitimately book for first class -- True " + "for " +
          str(first_cnt) + " times " + "\n")  # should be 2

    business_cnt = 0
    for i in range(2):
        if a2.book_seat(4, 9, "business"):
            business_cnt += 1
    print("Legitimately book for business class -- True " + "for " +
          str(business_cnt) + " times " + "\n")  # should be 2

    print("Legitimately book for economy class -- " +
          str(a2.book_seat(4, 9, "economy")) + "\n")  # should be True


## You can put testing code in here. It will not affect our autotester.
if __name__ == '__main__':
    # TODO: Put your testing code here, or call testing functions such as
    # this one:
    sample_testing_function()
    testing_function_1()
    testing_function_2()
