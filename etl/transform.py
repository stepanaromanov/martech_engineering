import importlib
import pandas as pd
import numpy as np
time_functions = importlib.import_module("utils.time_functions")

def services_rooms(object):
    groups_data_arrray = []
    # iterate over rows and append information to groups data array
    for elem in object:
        row = [elem['id'],
               elem['branch_id'],
               elem['company_group_number'],
               elem['code'],
               elem['name'],
               elem['days'],
               elem['lesson_start_time'],
               elem['lesson_end_time'],
               elem['status'],
               elem['type'],
               elem['group_start_date'],
               elem['group_end_date'],
               elem['student_count']
               ]

        # create dictionary from course object and append its data
        if len(elem['course']) > 0:
            course = {k: v for (k, v) in zip(elem['course'].keys(), elem['course'].values())}
            row += course['id'], \
                course['name'], \
                course['description'], \
                course['lesson_duration'], \
                course['course_duration'], \
                course['price'], \
                course['type'], \
                course['is_enabled']
        else:
            row += '0', \
                'unknown', \
                'unknown', \
                '0', \
                '0', \
                '0', \
                '0', \
                '0'

        # create dictionary from room object and append its data
        if len(elem['rooms']) > 0:
            room = {k: v for (k, v) in zip(elem['rooms'][0].keys(), elem['rooms'][0].values())}
            row += room['id'], \
                room['name']
        else:
            row += '0', \
                'unknown'

        # create dictionary from teacher object and append its data
        if len(elem['teachers']) > 0:
            teacher = {k: v for (k, v) in zip(elem['teachers'][0].keys(), elem['teachers'][0].values())}
            row += teacher['id'], \
                teacher['name']
        else:
            row += '0', \
                'unknown'

        # add the whole row to array and reset to zero
        groups_data_arrray.append(row)
        row = []

    # create dataframe from columns and groups data
    dataframe = pd.DataFrame(
        columns=[
            'id',
            'branch_id',
            'company_group_number',
            'code',
            'name',
            'days',
            'lesson_start_time',
            'lesson_end_time',
            'status',
            'type',
            'group_start_date',
            'group_end_date',
            'student_count',
            'course_id',
            'course_name',
            'course_description',
            'course_lesson_duration',
            'course_duration',
            'course_price',
            'course_type',
            'course_is_enabled',
            'room_id',
            'room_name',
            'teacher_id',
            'teacher_name'
        ],
        data=groups_data_arrray
    )

    dataframe.replace("'", "", inplace=True, regex=True)

    dataframe['group_start_date'] = pd.to_datetime(dataframe['group_start_date'])
    dataframe['group_end_date'] = pd.to_datetime(dataframe['group_end_date'])

    # create main dataframe for database
    data_to_insert = pd.DataFrame(columns=['date', 'id', 'name', 'days',
       'lesson_start_time', 'group_start_date', 'group_end_date',
       'student_count', 'course_name', 'course_duration', 'course_price',
       'room_id', 'room_name', 'teacher_id', 'teacher_name', 'capacity'], data=[])

    # loop through the rows using iterrows() method
    for index, row in dataframe.iterrows():
        # create date range for all the dates between start date and end date for each course
        date_range = pd.date_range(start=row['group_start_date'], end=row['group_end_date'], freq='D')

        # create new rows for each date in date range if it is odd or even weekday, depending on the course group
        new_rows = [{'date': date,
             'id': row['id'],
             'name': row['name'],
             'days': row['days'],
             'lesson_start_time': row['lesson_start_time'],
             'group_start_date': row['group_start_date'],
             'group_end_date': row['group_end_date'],
             'student_count': row['student_count'],
             'course_name': row['course_name'],
             'course_duration': row['course_duration'],
             'course_price': row['course_price'],
             'room_id': row['room_id'],
             'room_name': row['room_name'],
             'teacher_id': row['teacher_id'],
             'teacher_name': row['teacher_name'],
             'capacity': 13
             } for date in date_range \
            if (date.weekday() in (0, 2, 4) and row['days'] == 1) or \
            (date.weekday() in (1, 3, 5) and row['days'] == 2)]

        # create temporary dataframe from the rows
        temp_dataframe = pd.DataFrame(new_rows)

        # append temporary dataframe to the dataframe for sql operations
        if not data_to_insert.empty and not data_to_insert.isna().all(axis=1).all():
            # Concatenate dataframes vertically
            data_to_insert = pd.concat([data_to_insert, temp_dataframe], ignore_index=True)
        else:
            # during first iteration we just copy temp_dataframe
            data_to_insert = temp_dataframe.copy()

    # add new time columns
    data_to_insert['week'] = data_to_insert['date'].dt.isocalendar().week
    data_to_insert['month'] = data_to_insert['date'].dt.month
    data_to_insert['year'] = data_to_insert['date'].dt.year
    data_to_insert['date_range'] = data_to_insert.apply(
        lambda row: time_functions.get_date_range_from_week(row['year'], row['week']), axis=1
    )
    data_to_insert['date'] = data_to_insert['date'].astype(str).str[:10]

    # create local copy of the data
    data_to_insert.to_csv(
        f"data_archive/services_rooms__{time_functions.get_current_datetime()}.csv",
        index=False
    )

    return data_to_insert


def sales_crm_leads(leads_object):
    # data file has a nested structure which looks like:
    # array of objects ->
    # data fields and sections (array of objects) ->
    # data fields and leads (array of objects) ->
    # data fields and object linked_staff_id

    leads_data = []

    for column in leads_object:
        # adding category information
        row = [column['id'], str(column['name']).replace("'", "")]
        for section in column['sections']:
            # adding subcategory information
            row += [section['id'], str(section['name']).replace("'", "")]
            for lead in section['leads']:
                # adding lead information
                if lead['linked_stuff_id']:
                    staff_name = lead['linked_stuff_id']['name']
                else:
                    staff_name = None
                if not lead['linked_stuff_id']:
                    staff_name = None
                row += [lead['id'],
                        lead['order_number'],
                        str(lead['name']).replace("'", ""),
                        lead['phone'],
                        str(lead['comment']).replace("'", ""),
                        lead['created_by'],
                        lead['updated_by'],
                        lead['deleted_by'],
                        str(lead['deleted_at']),
                        str(lead['created_at']),
                        str(lead['updated_at']),
                        lead['course_id'],
                        lead['source_id'],
                        str(staff_name).replace("'", "")
                        ]
                leads_data.append(row)
                # truncating lead information after lead row adding
                row = row[:4]
            # truncating section information after all leads added
            row = row[:2]

    leads_data = pd.DataFrame(columns=["category_id",
                                    "category_name",
                                    "subcategory_id",
                                    "subcategory_name",
                                    "lead_id",
                                    "order_number",
                                    "name",
                                    "phone",
                                    "comment",
                                    "created_by",
                                    "updated_by",
                                    "deleted_by",
                                    "deleted_at",
                                    "created_at",
                                    "updated_at",
                                    "course_id",
                                    "source_id",
                                    "staff_name"],
                            data=leads_data)

    # change data types and fill not available values
    leads_data['order_number'] = leads_data['order_number'].astype(int)
    leads_data['created_by'] = leads_data['created_by'].fillna(0).astype(int)
    leads_data['updated_by'] = leads_data['updated_by'].fillna(0).astype(int)
    leads_data['course_id'] = leads_data['course_id'].fillna(0).astype(int)
    leads_data['source_id'] = leads_data['source_id'].fillna(0).astype(int)
    leads_data['deleted_by'] = leads_data['deleted_by'].fillna(0).astype(int)
    leads_data['deleted_at'] = leads_data['deleted_at'].astype(str)
    leads_data['staff_name'] = leads_data['staff_name'].fillna('unknown')
    leads_data['created_at'] = pd.to_datetime(leads_data['created_at']).dt.date
    leads_data['updated_at'] = pd.to_datetime(leads_data['updated_at']).dt.date


    # create local copy of the data
    leads_data.to_csv(
        f"data_archive/sales_crm_leads__{time_functions.get_current_datetime()}.csv",
        index=False
    )

    return leads_data