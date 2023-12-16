# pip install psycopg2-binary
import json
import psycopg2
from psycopg2 import IntegrityError
from psycopg2 import OperationalError
from psycopg2 import sql


def postgres_connect():
    with open('configuration/credentials/postgres.json') as db:
        db_credentials = json.load(db)
    try:
        conn = psycopg2.connect(
            host=db_credentials["host"],
            port=db_credentials["port"],
            database=db_credentials["database"],
            user=db_credentials["user"],
            password=db_credentials["password"],
        )
        return conn
    except Exception as error:
        print(f"Error while connecting to database: {error}")


def services_rooms(dataframe):
    try:
        conn = postgres_connect()
        cur = conn.cursor()

        # create table if it doesn't exist
        # it would be time-consuming (15x) to compare existing groups row over row so we truncate table if it does exist to insert rooms data
        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS services_rooms (
                    date text,
                    week numeric,
                    month numeric,
                    year numeric,
                    date_range text,
                    id numeric, 
                    name text, 
                    days numeric, 
                    lesson_start_time text, 
                    group_start_date text,
                    group_end_date text, 
                    student_count numeric, 
                    course_name text, 
                    course_duration numeric,
                    course_price numeric, 
                    room_id numeric, 
                    room_name text, 
                    teacher_id numeric, 
                    teacher_name text,
                    capacity numeric, 
                    last_updated timestamp
                );
                TRUNCATE TABLE services_rooms;
            """
        )
        conn.commit()

        all_rows = len(dataframe)
        inserted_rows = 0

        for index, row in dataframe.iterrows():
            try:
                # proceed with insertion
                cur.execute(
                    f"""
                        INSERT INTO services_rooms (
                            date,
                            week,
                            month,
                            year,
                            date_range,
                            id, 
                            name, 
                            days, 
                            lesson_start_time, 
                            group_start_date,
                            group_end_date, 
                            student_count, 
                            course_name, 
                            course_duration,
                            course_price, 
                            room_id, 
                            room_name, 
                            teacher_id, 
                            teacher_name,
                            capacity,
                            last_updated
                        ) VALUES (
                            %(date)s,
                            %(week)s,
                            %(month)s,
                            %(year)s,
                            %(date_range)s,
                            %(id)s, 
                            %(name)s, 
                            %(days)s, 
                            %(lesson_start_time)s, 
                            %(group_start_date)s,
                            %(group_end_date)s, 
                            %(student_count)s, 
                            %(course_name)s, 
                            %(course_duration)s,
                            %(course_price)s, 
                            %(room_id)s, 
                            %(room_name)s, 
                            %(teacher_id)s, 
                            %(teacher_name)s,
                            %(capacity)s,
                            NOW()::timestamp
                        );""", row
                )
                inserted_rows += 1

                conn.commit()

            except IntegrityError as error:
                conn.rollback()
                print(f"The error '{error}' occurred")

        cur.close()
        conn.close()

        print(f"===== SERVICES ROOMS CAPACITY ========================================================")
        print(f"All the rows in dataframe: {all_rows}")
        print(f"New inserted rows in database: {inserted_rows}")
        print("Data is successfully extracted, transformed, and loaded into database")
        print("======================================================================================")
        print("\n")

    except OperationalError as error:
        print(f"The error '{error}' occurred")



def finance_costs(costs, staff_costs, month, year):
    try:
        conn = postgres_connect()
        cur = conn.cursor()
        # create 2 new tables
        cur.execute(
            # we do not have unique ids in the original monthly costs tables and their size is relatively small
            # we truncate every table if it does exist to avoid errors
            f"""
                CREATE TABLE IF NOT EXISTS accounting_{year}_{month}_costs (
                    sana text,
                    xarajat_mazmuni text,
                    summa numeric,
                    manba text,
                    kategoriya text,
                    last_updated timestamp
                );
                CREATE TABLE IF NOT EXISTS accounting_{year}_{month}_staff_costs (
                    sana text,
                    xarajat_turi text,
                    xarajat_mazmuni text,
                    summa numeric,
                    manba text,
                    kategoriya text,
                    last_updated timestamp
                );
                TRUNCATE TABLE accounting_{year}_{month}_costs;
                TRUNCATE TABLE accounting_{year}_{month}_staff_costs;
            """
        )
        conn.commit()

        all_rows_costs = len(costs)
        inserted_rows_costs = 0

        all_rows_salary_costs = len(staff_costs)
        inserted_rows_salary_costs = 0

        # if data already exists we add new rows in case financial information is updated
        for index, row in costs.iterrows():
            try:
                # proceed with insertion
                cur.execute(
                    f"""
                        INSERT INTO accounting_{year}_{month}_costs (
                            sana,
                            xarajat_mazmuni,
                            summa,
                            manba,
                            kategoriya,
                            last_updated
                        ) VALUES (
                            %(sana)s,
                            %(xarajat_mazmuni)s,
                            %(summa)s,
                            %(manba)s,
                            %(kategoriya)s,
                            NOW()::timestamp
                        );
                    """, row
                )
                inserted_rows_costs += 1

                conn.commit()

            except IntegrityError as error:
                conn.rollback()
                print(f"The error '{error}' occurred")

        for index, row in staff_costs.iterrows():
            try:
                #proceed with insertion
                cur.execute(
                    f"""
                        INSERT INTO accounting_{year}_{month}_staff_costs (
                            sana,
                            xarajat_turi,
                            xarajat_mazmuni,
                            summa,
                            manba,
                            kategoriya,
                            last_updated
                        ) VALUES (
                            %(sana)s,
                            %(xarajat_turi)s,
                            %(xarajat_mazmuni)s,
                            %(summa)s,
                            %(manba)s,
                            %(kategoriya)s,
                            NOW()::timestamp
                        );
                    """, row
                )

                inserted_rows_salary_costs += 1

                conn.commit()

            except IntegrityError as error:
                conn.rollback()
                print(f"The error '{error}' occurred")

        cur.close()
        conn.close()

        print(f"===== FINANCE COSTS ==================================================================")
        print(f"Dataframe 'Costs':")
        print(f"All the rows in dataframe: {all_rows_costs}")
        print(f"New inserted rows in database: {inserted_rows_costs}")
        print("-----------------------------------------------------------")
        print(f"Dataframe 'Salary Costs':")
        print(f"All the rows in dataframe: {all_rows_salary_costs}")
        print(f"New inserted rows in database: {inserted_rows_salary_costs}")
        print("Data is successfully extracted, transformed, and loaded into database")
        print("======================================================================================")
        print("\n")

    except OperationalError as error:
        print(f"The error '{error}' occurred")


def finance_client_groups(year, dataframe):
    try:
        conn = postgres_connect()
        cur = conn.cursor()

        cur.execute(
            f"""
                CREATE TABLE IF NOT EXISTS accounting_{year}_groups_main (
                    year numeric,
                    month text,
                    lead_id numeric,
                    phone_number text,
                    full_name text,
                    group_name text,
                    date text,
                    course_price numeric,
                    total_amount numeric,
                    paid_amount numeric,
                    discount_amount numeric,
                    agency text,
                    overpaid_amount numeric,
                    expected_income text,
                    teacher text,
                    last_updated timestamp
                );
            """
        )
        conn.commit()

        all_rows = len(dataframe)
        updated_rows = 0
        inserted_rows = 0

        # if data already exists we add new rows in case financial information is updated
        for index, row in dataframe.iterrows():
            try:
                # check for existing information
                cur.execute(
                    f"""
                        SELECT * 
                        FROM accounting_{year}_groups_main
                        WHERE phone_number = %(phone_number)s
                            AND full_name = %(full_name)s
                            AND month = %(month)s
                    """, row
                )

                # If there are existing duplicate information about lead, we update the values
                if cur.fetchone():
                    cur.execute(
                        f"""
                            UPDATE accounting_{year}_groups_main
                            SET year = %(year)s,
                                lead_id = %(lead_id)s,
                                group_name = %(group_name)s,
                                date = %(date)s,
                                course_price = %(course_price)s,
                                total_amount = %(total_amount)s,
                                paid_amount = %(paid_amount)s,
                                discount_amount = %(discount_amount)s,
                                agency = %(agency)s,
                                overpaid_amount = %(overpaid_amount)s,
                                expected_income = %(expected_income)s,
                                teacher = %(teacher)s,
                                last_updated = NOW()::timestamp
                            WHERE phone_number = %(phone_number)s
                                AND full_name = %(full_name)s
                                AND month = %(month)s
                        """, row
                    )
                    updated_rows += 1

                else:
                    # no conflict, proceed with insertion
                    cur.execute(
                        f"""
                            INSERT INTO accounting_{year}_groups_main (
                                year,
                                month,
                                lead_id,
                                phone_number,
                                full_name,
                                group_name,
                                date,
                                course_price,
                                total_amount,
                                paid_amount,
                                discount_amount,
                                agency,
                                overpaid_amount,
                                expected_income,
                                teacher,
                                last_updated
                            ) VALUES (
                                %(year)s,
                                %(month)s,
                                %(lead_id)s,
                                %(phone_number)s,
                                %(full_name)s,
                                %(group_name)s,
                                %(date)s,
                                %(course_price)s,
                                %(total_amount)s,
                                %(paid_amount)s,
                                %(discount_amount)s,
                                %(agency)s,
                                %(overpaid_amount)s,
                                %(expected_income)s,
                                %(teacher)s,
                                NOW()::timestamp
                            );""", row
                    )
                    inserted_rows += 1

                conn.commit()

            except IntegrityError as error:
                conn.rollback()
                print(f"The error '{error}' occurred")

        cur.close()
        conn.close()

        print(f"===== FINANCE CLIENT GROUPS ==========================================================")
        print(f"All the rows in dataframe: {all_rows}")
        print(f"Updated rows due to conflict with existing ones: {updated_rows}")
        print(f"New inserted rows in database: {inserted_rows}")
        print("Data is successfully extracted, transformed, and loaded into database")
        print("======================================================================================")
        print("\n")

    except OperationalError as error:
        print(f"The error '{error}' occurred")


def marketing_daily_leads(dataframe):
    try:
        conn = postgres_connect()
        cur = conn.cursor()

        # create a new table
        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS marketing_daily_leads_2023 (
                    date text not null,
                    it_reception_leads numeric,
                    it_telegram_leads numeric,
                    it_instagram_leads numeric,
                    english_reception_leads numeric,
                    english_telegram_leads numeric,
                    english_instagram_leads numeric,
                    kids_reception_leads numeric,
                    kids_telegram_leads numeric,
                    kids_instagram_leads numeric,
                    last_updated timestamp not null
                );
            """
        )

        conn.commit()

        all_rows = len(dataframe)
        skipped_rows = 0
        inserted_rows = 0

        for index, row in dataframe.iterrows():
            try:
                # check for conflicts (full duplicates) before inserting
                select_query = sql.SQL(
                    """
                        SELECT date 
                        FROM marketing_daily_leads_2023 
                        WHERE date = %s
                            AND it_reception_leads = %s
                            AND it_telegram_leads = %s
                            AND it_instagram_leads = %s
                            AND english_reception_leads = %s
                            AND english_telegram_leads = %s
                            AND english_instagram_leads = %s
                            AND kids_reception_leads = %s
                            AND kids_telegram_leads = %s
                            AND kids_instagram_leads = %s
                    """
                )

                cur.execute(select_query, (
                       row['Date'],
                       row['IT Reception leads'],
                       row['IT Telegram leads'],
                       row['IT Instagram leads'],
                       row['English Reception leads'],
                       row['English Telegram leads'],
                       row['English Instagram leads'],
                       row['Kids Reception leads'],
                       row['Kids Telegram leads'],
                       row['Kids Instagram leads']
                       )
                )

                if cur.fetchone():
                    # conflict: data with similar values already exists
                    skipped_rows += 1

                else:
                    # no conflict, proceed with insertion; delete rows with incomplete data about leads count
                    inserted_rows += 1
                    cur.execute(
                        f"""
                            DELETE FROM marketing_daily_leads_2023 WHERE date = '{row["Date"]}';
                        """
                    )

                    cur.execute(
                        f"""
                            INSERT INTO marketing_daily_leads_2023 (
                                date,
                                it_reception_leads,
                                it_telegram_leads,
                                it_instagram_leads,
                                english_reception_leads,
                                english_telegram_leads,
                                english_instagram_leads,
                                kids_reception_leads,
                                kids_telegram_leads,
                                kids_instagram_leads,
                                last_updated
                            ) VALUES (
                                %(Date)s,
                                %(IT Reception leads)s,
                                %(IT Telegram leads)s,
                                %(IT Instagram leads)s,
                                %(English Reception leads)s,
                                %(English Telegram leads)s,
                                %(English Instagram leads)s,
                                %(Kids Reception leads)s,
                                %(Kids Telegram leads)s,
                                %(Kids Instagram leads)s,
                                NOW()::timestamp
                            );
                        """, row
                    )

                conn.commit()

            except IntegrityError as error:
                conn.rollback()
                print(f"The error '{error}' occurred")

        cur.close()
        conn.close()

        print(f"===== MARKETING DAILY LEADS =========================================================")
        print(f"All the rows in dataframe: {all_rows}")
        print(f"Skipped rows due to conflict with existing ones: {skipped_rows}")
        print(f"New inserted rows in database: {inserted_rows}")
        print("Data is successfully extracted, transformed, and loaded into database")
        print("======================================================================================")
        print("\n")


    except OperationalError as error:
        print(f"The error '{error}' occurred")


def marketing_facebook_instagram_spending(dataframe):
    try:
        conn = postgres_connect()
        cur = conn.cursor()

        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS facebook_ads (
                    campaign_id text not null,
                    campaign_name text not null,
                    spend decimal,
                    date_start text not null,
                    date_stop text not null,
                    clicks numeric,
                    cpc numeric,
                    cpm numeric,
                    cpp numeric,
                    ctr numeric,
                    frequency numeric,
                    engagement_rate_ranking text,
                    impressions numeric,
                    quality_ranking text,
                    reach numeric,
                    objective text,
                    last_updated timestamp
                );
            """
        )
        conn.commit()

        all_rows = len(dataframe)
        updated_rows = 0
        inserted_rows = 0

        for index, row in dataframe.iterrows():
            try:
                cur.execute(
                        f"""
                            SELECT campaign_id 
                            FROM facebook_ads
                            WHERE campaign_id = '{row['campaign_id']}'
                         """
                )

                if cur.fetchone():
                    # conflict: data with similar values already exists
                    cur.execute(
                        # information on spending is updated so we will delete old rows where campaign ids intersect; then we will insert new data
                        f"""
                            DELETE FROM facebook_ads
                            WHERE campaign_id = %(campaign_id)s;
                            INSERT INTO facebook_ads (
                                campaign_id, 
                                campaign_name, 
                                spend, 
                                date_start, 
                                date_stop,
                                clicks,
                                cpc,
                                cpm,
                                cpp,
                                ctr,
                                frequency,
                                engagement_rate_ranking,
                                impressions,
                                quality_ranking,
                                reach,
                                objective,
                                last_updated
                            ) VALUES (
                                %(campaign_id)s,
                                %(campaign_name)s,
                                %(spend)s,
                                %(date_start)s,
                                %(date_stop)s,
                                %(clicks)s,
                                %(cpc)s,
                                %(cpm)s,
                                %(cpp)s,
                                %(ctr)s,
                                %(frequency)s,
                                %(engagement_rate_ranking)s,
                                %(impressions)s,
                                %(quality_ranking)s,
                                %(reach)s,
                                %(objective)s,
                                NOW()::timestamp
                        );""", row
                    )

                    updated_rows += 1

                else:
                    # no conflict, proceed with insertion
                    cur.execute(
                        f"""
                            INSERT INTO facebook_ads (
                                campaign_id, 
                                campaign_name, 
                                spend, 
                                date_start, 
                                date_stop,
                                clicks,
                                cpc,
                                cpm,
                                cpp,
                                ctr,
                                frequency,
                                engagement_rate_ranking,
                                impressions,
                                quality_ranking,
                                reach,
                                objective,
                                last_updated
                            ) VALUES (
                                %(campaign_id)s,
                                %(campaign_name)s,
                                %(spend)s,
                                %(date_start)s,
                                %(date_stop)s,
                                %(clicks)s,
                                %(cpc)s,
                                %(cpm)s,
                                %(cpp)s,
                                %(ctr)s,
                                %(frequency)s,
                                %(engagement_rate_ranking)s,
                                %(impressions)s,
                                %(quality_ranking)s,
                                %(reach)s,
                                %(objective)s,
                                NOW()::timestamp
                        );""", row
                    )

                    inserted_rows += 1

                conn.commit()

            except IntegrityError as error:
                conn.rollback()
                print(f"The error '{error}' occurred")

        cur.close()
        conn.close()

        print(f"===== MARKETING FACEBOOK INSTAGRAM SPENDING =========================================")
        print(f"All the rows in dataframe: {all_rows}")
        print(f"Updated existing rows in database : {updated_rows}")
        print(f"New inserted rows in database: {inserted_rows}")
        print("Data is successfully extracted, transformed, and loaded into database")
        print("======================================================================================")
        print("\n")

    except OperationalError as error:
        print(f"The error '{error}' occurred")


def marketing_google_analytics_sessions(dataframe):
    try:
        conn = postgres_connect()
        cur = conn.cursor()

        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS ga_datatalim_sessions (
                    year numeric,
                    month numeric,
                    week numeric,
                    session_medium text,
                    session_source text,
                    session_campaign_name text,
                    session_manual_ad_content text,
                    session_manual_term text,
                    average_session_duration numeric,
                    active_users numeric,
                    bounce_rate numeric,
                    event_count numeric,
                    engagement_rate numeric,
                    conversions numeric,
                    date_range text,
                    last_updated timestamp
                );
            """
        )
        conn.commit()

        all_rows = len(dataframe)
        updated_rows = 0
        inserted_rows = 0

        for index, row in dataframe.iterrows():
            try:
                # check for existing dimensions
                cur.execute(
                    f"""
                        SELECT * 
                        FROM ga_datatalim_sessions
                        WHERE week = %(week)s
                            AND year = %(year)s
                            AND month = %(month)s
                            AND session_medium = %(sessionMedium)s
                            AND session_source = %(sessionSource)s
                            AND session_campaign_name = %(sessionCampaignName)s
                            AND session_manual_ad_content = %(sessionManualAdContent)s
                            AND session_manual_term = %(sessionManualTerm)s
                    """, row
                )

                # If there are existing duplicate dimensions, we update the metrics
                if cur.fetchone():
                    cur.execute(
                        f"""
                            UPDATE ga_datatalim_sessions
                            SET average_session_duration = %(averageSessionDuration)s,
                                active_users = %(activeUsers)s,
                                bounce_rate = %(bounceRate)s,
                                event_count = %(eventCount)s,
                                engagement_rate = %(engagementRate)s,
                                conversions = %(conversions)s,
                                date_range = %(dateRange)s,
                                last_updated = NOW()::timestamp
                            WHERE week = %(week)s
                                AND year = %(year)s
                                AND month = %(month)s
                                AND session_medium = %(sessionMedium)s
                                AND session_source = %(sessionSource)s
                                AND session_campaign_name = %(sessionCampaignName)s
                                AND session_manual_ad_content = %(sessionManualAdContent)s
                                AND session_manual_term = %(sessionManualTerm)s
                        """, row
                    )
                    updated_rows += 1

                else:
                    # no conflict, proceed with insertion
                    cur.execute(
                        f"""
                            INSERT INTO ga_datatalim_sessions (
                                year,
                                month,
                                week,
                                session_medium,
                                session_source,
                                session_campaign_name,
                                session_manual_ad_content,
                                session_manual_term,
                                average_session_duration,
                                active_users,
                                bounce_rate,
                                event_count,
                                engagement_rate,
                                conversions,
                                date_range,
                                last_updated
                            ) VALUES (
                                %(year)s,
                                %(month)s,
                                %(week)s,
                                %(sessionMedium)s,
                                %(sessionSource)s,
                                %(sessionCampaignName)s,
                                %(sessionManualAdContent)s,
                                %(sessionManualTerm)s,
                                %(averageSessionDuration)s,
                                %(activeUsers)s,
                                %(bounceRate)s,
                                %(eventCount)s,
                                %(engagementRate)s,
                                %(conversions)s,
                                %(dateRange)s,
                                NOW()::timestamp
                            );""", row
                    )
                    inserted_rows += 1

                conn.commit()

            except IntegrityError as error:
                conn.rollback()
                print(f"The error '{error}' occurred")

        cur.close()
        conn.close()

        print(f"===== MARKETING GA4 SESSIONS =========================================================")
        print(f"All the rows in dataframe: {all_rows}")
        print(f"Updated existing rows in database : {updated_rows}")
        print(f"New inserted rows in database: {inserted_rows}")
        print("Data is successfully extracted, transformed, and loaded into database")
        print("======================================================================================")
        print("\n")

    except OperationalError as error:
        print(f"The error '{error}' occurred")


def marketing_google_analytics_devices(dataframe):
    try:
        conn = postgres_connect()
        cur = conn.cursor()

        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS ga_datatalim_devices (
                    year numeric,
                    month numeric,
                    week numeric,
                    date_hour text,
                    day_of_week numeric,
                    device_category text,
                    device_model text,
                    average_session_duration numeric,
                    active_users numeric,
                    bounce_rate numeric,
                    event_count numeric,
                    engagement_rate numeric,
                    conversions numeric,
                    date_range text,
                    last_updated timestamp
                );
            """
        )
        conn.commit()

        all_rows = len(dataframe)
        updated_rows = 0
        inserted_rows = 0

        for index, row in dataframe.iterrows():
            try:
                # check for existing dimensions
                cur.execute(
                    f"""
                        SELECT * 
                        FROM ga_datatalim_devices
                        WHERE week = %(week)s
                            AND year = %(year)s
                            AND month = %(month)s
                            AND date_hour = %(dateHour)s
                            AND day_of_week = %(dayOfWeek)s
                            AND device_category = %(deviceCategory)s
                            AND device_model = %(deviceModel)s
                    """, row
                )

                # If there are existing duplicate dimensions, we update the metrics
                if cur.fetchone():
                    cur.execute(
                        f"""
                            UPDATE ga_datatalim_devices
                            SET average_session_duration = %(averageSessionDuration)s,
                                active_users = %(activeUsers)s,
                                bounce_rate = %(bounceRate)s,
                                event_count = %(eventCount)s,
                                engagement_rate = %(engagementRate)s,
                                conversions = %(conversions)s,
                                date_range = %(dateRange)s,
                                last_updated = NOW()::timestamp
                            WHERE week = %(week)s
                                AND year = %(year)s
                                AND month = %(month)s
                                AND date_hour = %(dateHour)s
                                AND day_of_week = %(dayOfWeek)s
                                AND device_category = %(deviceCategory)s
                                AND device_model = %(deviceModel)s
                        """, row
                    )
                    updated_rows += 1

                else:
                    # no conflict, proceed with insertion
                    cur.execute(
                        f"""
                            INSERT INTO ga_datatalim_devices (
                                year,
                                month,
                                week,
                                date_hour,
                                day_of_week,
                                device_category,
                                device_model,
                                average_session_duration,
                                active_users,
                                bounce_rate,
                                event_count,
                                engagement_rate,
                                conversions,
                                date_range,
                                last_updated
                            ) VALUES (
                                %(year)s,
                                %(month)s,
                                %(week)s,
                                %(dateHour)s,
                                %(dayOfWeek)s,
                                %(deviceCategory)s,
                                %(deviceModel)s,
                                %(averageSessionDuration)s,
                                %(activeUsers)s,
                                %(bounceRate)s,
                                %(eventCount)s,
                                %(engagementRate)s,
                                %(conversions)s,
                                %(dateRange)s,
                                NOW()::timestamp
                            );""", row
                    )
                    inserted_rows += 1

                conn.commit()

            except IntegrityError as error:
                conn.rollback()
                print(f"The error '{error}' occurred")

        cur.close()
        conn.close()

        print(f"===== MARKETING GA4 DEVICES ==========================================================")
        print(f"All the rows in dataframe: {all_rows}")
        print(f"Updated existing rows in database : {updated_rows}")
        print(f"New inserted rows in database: {inserted_rows}")
        print("Data is successfully extracted, transformed, and loaded into database")
        print("======================================================================================")
        print("\n")

    except OperationalError as error:
        print(f"The error '{error}' occurred")


def marketing_google_analytics_page_paths(dataframe):
    try:
        conn = postgres_connect()
        cur = conn.cursor()

        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS ga_datatalim_page_paths (
                    year numeric,
                    month numeric,
                    week numeric,
                    page_path text,
                    active_users numeric,
                    date_range text,
                    last_updated timestamp
                );
            """
        )
        conn.commit()

        all_rows = len(dataframe)
        updated_rows = 0
        inserted_rows = 0

        for index, row in dataframe.iterrows():
            try:
                # check for existing dimensions
                cur.execute(
                    f"""
                        SELECT * 
                        FROM ga_datatalim_page_paths
                        WHERE week = %(week)s
                            AND year = %(year)s
                            AND month = %(month)s
                            AND page_path = %(pagePath)s
                    """, row
                )

                # If there are existing duplicate dimensions, we update the metrics
                if cur.fetchone():
                    cur.execute(
                        f"""
                            UPDATE ga_datatalim_page_paths
                            SET active_users = %(activeUsers)s,
                                date_range = %(dateRange)s,
                                last_updated = NOW()::timestamp
                            WHERE week = %(week)s
                            AND year = %(year)s
                            AND month = %(month)s
                            AND page_path = %(pagePath)s
                        """, row
                    )
                    updated_rows += 1

                else:
                    # no conflict, proceed with insertion
                    cur.execute(
                        f"""
                            INSERT INTO ga_datatalim_page_paths (
                                year,
                                month,
                                week,
                                page_path,
                                active_users,
                                date_range,
                                last_updated
                            ) VALUES (
                                %(year)s,
                                %(month)s,
                                %(week)s,
                                %(pagePath)s,
                                %(activeUsers)s,
                                %(dateRange)s,
                                NOW()::timestamp
                            );""", row
                    )
                    inserted_rows += 1

                conn.commit()

            except IntegrityError as error:
                conn.rollback()
                print(f"The error '{error}' occurred")

        cur.close()
        conn.close()

        print(f"===== MARKETING GA4 PAGE PATHS =======================================================")
        print(f"All the rows in dataframe: {all_rows}")
        print(f"Updated existing rows in database : {updated_rows}")
        print(f"New inserted rows in database: {inserted_rows}")
        print("Data is successfully extracted, transformed, and loaded into database")
        print("======================================================================================")
        print("\n")

    except OperationalError as error:
        print(f"The error '{error}' occurred")


def marketing_google_analytics_landing_pages(dataframe):
    try:
        conn = postgres_connect()
        cur = conn.cursor()

        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS ga_datatalim_landing_pages (
                    year numeric,
                    month numeric,
                    week numeric,
                    landing_page text,
                    active_users numeric,
                    date_range text,
                    last_updated timestamp
                );
            """
        )
        conn.commit()

        all_rows = len(dataframe)
        updated_rows = 0
        inserted_rows = 0

        for index, row in dataframe.iterrows():
            try:
                # check for existing dimensions
                cur.execute(
                    f"""
                        SELECT * 
                        FROM ga_datatalim_landing_pages
                        WHERE week = %(week)s
                            AND year = %(year)s
                            AND month = %(month)s
                            AND landing_page = %(landingPage)s
                    """, row
                )

                # If there are existing duplicate dimensions, we update the metrics
                if cur.fetchone():
                    cur.execute(
                        f"""
                            UPDATE ga_datatalim_landing_pages
                            SET active_users = %(activeUsers)s,
                                date_range = %(dateRange)s,
                                last_updated = NOW()::timestamp
                            WHERE week = %(week)s
                            AND year = %(year)s
                            AND month = %(month)s
                            AND landing_page = %(landingPage)s
                        """, row
                    )
                    updated_rows += 1

                else:
                    # no conflict, proceed with insertion
                    cur.execute(
                        f"""
                            INSERT INTO ga_datatalim_landing_pages (
                                year,
                                month,
                                week,
                                landing_page,
                                active_users,
                                date_range,
                                last_updated
                            ) VALUES (
                                %(year)s,
                                %(month)s,
                                %(week)s,
                                %(landingPage)s,
                                %(activeUsers)s,
                                %(dateRange)s,
                                NOW()::timestamp
                            );""", row
                    )
                    inserted_rows += 1

                conn.commit()

            except IntegrityError as error:
                conn.rollback()
                print(f"The error '{error}' occurred")

        cur.close()
        conn.close()

        print(f"===== MARKETING GA4 LANDING PAGES ====================================================")
        print(f"All the rows in dataframe: {all_rows}")
        print(f"Updated existing rows in database : {updated_rows}")
        print(f"New inserted rows in database: {inserted_rows}")
        print("Data is successfully extracted, transformed, and loaded into database")
        print("======================================================================================")
        print("\n")

    except OperationalError as error:
        print(f"The error '{error}' occurred")


def sales_crm_leads(dataframe):
    try:
        conn = postgres_connect()
        cur = conn.cursor()

        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS crm_leads (
                    category_id numeric,
                    category_name text,
                    subcategory_id numeric,
                    subcategory_name text,
                    lead_id numeric,
                    order_number numeric,
                    name text,
                    phone text,
                    comment text,
                    created_by numeric,
                    updated_by numeric,
                    deleted_by numeric,
                    deleted_at text,
                    created_at text,
                    updated_at text,
                    course_id numeric,
                    source_id numeric,
                    staff_name text,
                    last_updated timestamp
                );
            """
        )
        conn.commit()

        all_rows = len(dataframe)
        updated_rows = 0
        inserted_rows = 0

        for index, row in dataframe.iterrows():
            try:
                # check for existing leads
                cur.execute(
                    f"""
                        SELECT lead_id 
                        FROM crm_leads
                        WHERE lead_id = %(lead_id)s
                    """, row
                )

                # If there are existing lead ids, we update its data
                if cur.fetchone():
                    cur.execute(
                        f"""
                            UPDATE crm_leads
                            SET category_id = %(category_id)s,
                                category_name = %(category_name)s,
                                subcategory_id = %(subcategory_id)s,
                                subcategory_name = %(subcategory_name)s,
                                order_number = %(order_number)s,
                                name = %(name)s,
                                phone = %(phone)s,
                                comment = %(comment)s,
                                created_by = %(created_by)s,
                                updated_by = %(updated_by)s,
                                deleted_by = %(deleted_by)s,
                                deleted_at = %(deleted_at)s,
                                created_at = %(created_at)s,
                                updated_at = %(updated_at)s,
                                course_id = %(course_id)s,
                                source_id = %(source_id)s,
                                staff_name = %(staff_name)s,
                                last_updated = NOW()::timestamp
                            WHERE lead_id = %(lead_id)s
                        """, row
                    )
                    updated_rows += 1

                else:
                    # no conflict, proceed with insertion
                    cur.execute(
                        f"""
                            INSERT INTO crm_leads (
                                category_id,
                                category_name,
                                subcategory_id,
                                subcategory_name,
                                lead_id,
                                order_number,
                                name,
                                phone,
                                comment,
                                created_by,
                                updated_by,
                                deleted_by,
                                deleted_at,
                                created_at,
                                updated_at,
                                course_id,
                                source_id,
                                staff_name,
                                last_updated
                            ) VALUES (
                                %(category_id)s,
                                %(category_name)s,
                                %(subcategory_id)s,
                                %(subcategory_name)s,
                                %(lead_id)s,
                                %(order_number)s,
                                %(name)s,
                                %(phone)s,
                                %(comment)s,
                                %(created_by)s,
                                %(updated_by)s,
                                %(deleted_by)s,
                                %(deleted_at)s,
                                %(created_at)s,
                                %(updated_at)s,
                                %(course_id)s,
                                %(source_id)s,
                                %(staff_name)s,
                                NOW()::timestamp
                            );""", row
                    )
                    inserted_rows += 1

                conn.commit()

            except IntegrityError as error:
                conn.rollback()
                print(f"The error '{error}' occurred")

        cur.close()
        conn.close()

        print(f"===== SALES CRM LEADS ================================================================")
        print(f"All the rows in dataframe: {all_rows}")
        print(f"Updated existing rows in database : {updated_rows}")
        print(f"New inserted rows in database: {inserted_rows}")
        print("Data is successfully extracted, transformed, and loaded into database")
        print("======================================================================================")
        print("\n")

    except OperationalError as error:
        print(f"The error '{error}' occurred")