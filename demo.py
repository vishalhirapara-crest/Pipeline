"""Contains classes for mutations related to bulkchange."""
# Library imports
import json
from datetime import datetime

import graphene
from admin.coc.constants import COC_CODES

# Audit imports
from audit.audit import audit
from bson.objectid import ObjectId
from employee.exceptions import CompanySwitched

# Custom imports
from employee.models import COC, EmployeeDetails
from employee.utils import generate_cds_employee_id, get_highest_cds_employee_id
from flask import request

# Security imports
from flask_graphql_auth import AuthInfoField, mutation_header_jwt_required
from leave.constants import LeaveType
from security.jwt_auth import authorization, get_role_from_db, read_jwt
from security.objects import OkFieldObj
from tasks.change_business_group_notification import (
    send_change_business_group_bulk_notification,
)
from tasks.change_dm_notification import send_change_dm_bulk_notification
from tasks.designation_change_notification_task import (
    employee_designation_change_bulk_notification,
)

# Celery tasks imports
from tasks.employee_type_change_notification import (
    employee_type_change_bulk_notification,
)
from tasks.grade_change_notification_task import employee_grade_change_bulk_notification
from tasks.shift_change_notification_task import send_shift_change_bulk_notification
from util.logger_manager import get_logger_object

from .models import Leave
from .objects import GeneralFieldInputObj, LeaveFieldInputObj
from .utils import get_user_from_jwt, insert_designation_grade_history_from_bulkchange

logger_object = get_logger_object()


class BulkUnionObjects(graphene.Union):
    """Class related to union object for bulkchange."""

    class Meta:
        """Meta class of union object for bulkchange."""

        types = (OkFieldObj, AuthInfoField)

    @classmethod
    def resolve_type(cls, instance, info):
        """Resolve type method for bulkchange union object."""
        return type(instance)


def update_all_fields_for_bulk_change(hrms_ids, general_field_input, emp_details_list, employees):
    """Update fields for the bulk changes."""
    # Send notification to employee when their employee_type is changed
    if hrms_ids and ("employee_type" in general_field_input) and general_field_input["employee_type"]:
        employee_type_change_bulk_notification.delay(
            emp_details_list=emp_details_list, emp_type_code=general_field_input["employee_type"]
        )
    # Insert Designation Grade History when designation and grade changed
    if "grade" in general_field_input and "designation" in general_field_input:
        # if general_field_input contains "grade" and "designation"
        designation = COC.objects(coc_code=COC_CODES.DESIGNATION.value, code=general_field_input["designation"])
        grade_exist = False
        if designation:
            # Find the grade coc using the parent value from designation coc
            grade = COC.objects(coc_code=COC_CODES.GRADE.value, code=designation[0]["parent"])
            if grade:
                # Check if the grade code exist in the child of the grade coc object
                grade_exist = any(child.code == general_field_input["grade"] for child in grade[0]["child"])
            else:
                logger_object.error("grade does not exist for coc code {}".format(general_field_input["grade"]))
        else:
            logger_object.error("designation does not exist for coc code {}".format(general_field_input["designation"]))
        # checks that grade and designation are valid or not
        if grade_exist and designation:
            # create a designation grade history object using given input
            insert_designation_grade_history_from_bulkchange(employees, general_field_input)
            grade_and_designation_dict = {
                "grade": general_field_input["grade"],
                "designation": general_field_input["designation"],
            }
            result = employees.update(__raw__={"$set": grade_and_designation_dict})
            if result != len(hrms_ids):
                logger_object.error(
                    f"Error occured while updating the grade and designations. Number or records updated ({result}) does not match number of records ({len(hrms_ids)}) in dictionary"  # noqa E501
                )
                return False
            employee_designation_change_bulk_notification.delay(emp_details_list, general_field_input["designation"])
            employee_grade_change_bulk_notification.delay(
                emp_details_list, general_field_input["designation"], general_field_input["grade"]
            )
    # remove the grade and designation from general_field_input because this is already
    # inserted into db
    general_field_input.pop("grade", None)
    general_field_input.pop("designation", None)
    # Send notification to employees when their shift changes
    if hrms_ids and ("shift_type" in general_field_input) and general_field_input["shift_type"]:
        send_shift_change_bulk_notification.delay(emp_id_list=hrms_ids, emp_shift=general_field_input["shift_type"])

    # Send notification to employees when their business group changes
    if hrms_ids and ("business_group" in general_field_input) and general_field_input["business_group"]:
        send_change_business_group_bulk_notification.delay(
            emp_id_list=hrms_ids, emp_dept=general_field_input["business_group"]
        )

    # Send notification to employees when their direct manager changes
    if hrms_ids and ("direct_manager" in general_field_input) and general_field_input["direct_manager"]:
        employees.update(direct_manager=ObjectId(general_field_input["direct_manager"]))
        send_change_dm_bulk_notification.delay(
            emp_id_list=emp_details_list, emps_direct_manager=general_field_input["direct_manager"]
        )
    general_field_input.pop("direct_manager", None)
    # if Company name is updated, generate the Cds employee id
    ok = True
    emp_error_list = []
    if "saral" in general_field_input:
        # Fetch the object that has the highest Cds employee id
        current_high = get_highest_cds_employee_id(general_field_input["saral"])
        for employee in employees:
            is_company_name_changed = employee.saral != general_field_input["saral"]
            if is_company_name_changed and employee.cds_code == "":
                new_high = generate_cds_employee_id(current_high)
                current_high = new_high
                employee_obj = EmployeeDetails.objects(_id=employee._id)
                employee_obj.update(cds_code=new_high, saral=general_field_input["saral"])
            elif is_company_name_changed:
                logger_object.error("Error occurred while updating saral for {}".format(employee._id))
                emp_error_list.append(employee._id)
                ok = False
    general_field_input.pop("saral", None)
    # check that general_field_input dictionary is not empty
    if bool(general_field_input):
        employees.update(__raw__={"$set": general_field_input})

    if ok:
        return True
    raise CompanySwitched("Company Name Cannot be updated for {}".format(emp_error_list))


def update_dm_and_buissness_group_of_the_user(hrms_ids, general_field_input, emp_details_list, employees):
    """Update direct manager and Buissness group of the user."""
    if hrms_ids and ("direct_manager" in general_field_input) and general_field_input["direct_manager"]:
        employees.update(__raw__={"$set": {"direct_manager": ObjectId(general_field_input["direct_manager"])}})
        send_change_dm_bulk_notification.delay(
            emp_id_list=emp_details_list, emps_direct_manager=general_field_input["direct_manager"]
        )

    if hrms_ids and ("business_group" in general_field_input) and general_field_input["business_group"]:
        employees.update(__raw__={"$set": {"business_group": general_field_input["business_group"]}})
        send_change_business_group_bulk_notification.delay(
            emp_id_list=hrms_ids, emp_dept=general_field_input["business_group"]
        )


def update_leave_balance_of_the_user(leave_field_input, leave_input_value, employees):
    """Update leave balance of the user."""
    leave_history_obj = {
        "date": str(datetime.now()),
        "credit": leave_input_value if leave_field_input.operation == "ADD_TO_EXISTING" else -leave_input_value,
    }
    # Object to push history object and set leave balance.
    update_queries_obj = {
        "ADD_TO_EXISTING": {
            "$inc": {"master_leave_balance.$.leave_balance": leave_input_value},
            "$push": {"master_leave_balance.$.history": leave_history_obj},
        },
        "REMOVE_FROM_EXISTING": {
            "$inc": {"master_leave_balance.$.leave_balance": -leave_input_value},
            "$push": {"master_leave_balance.$.history": leave_history_obj},
        },
    }
    # If bulk change operation is replace all with
    if leave_field_input.operation == "REPLACE_ALL_WITH":
        for emp in employees:
            leave_history_obj = {
                "date": str(datetime.now()),
                "credit": leave_input_value
                - float(emp["master_leave_balance"][0]["leave_balance"])
                + float(emp["master_leave_balance"][0]["leave_taken"]),
            }
            emp.update(
                __raw__={
                    "$set": {
                        "master_leave_balance.0.leave_balance": leave_input_value
                        + float(emp["master_leave_balance"][0]["leave_taken"])
                    },
                    "$push": {"master_leave_balance.0.history": leave_history_obj},
                }
            )
    else:
        # If bulk change operation is not replace all with.
        employees.update(__raw__=update_queries_obj[leave_field_input.operation])


def get_leave_input_value(leave_field_input):
    """Get leave balance input from mutation field.

    Args:
        leave_field_input (LeaveFieldInputObj): Contains input values of leave field of bulkchange mutation.

    Returns:
        float: Input value for bulk leave balance change
        None: If leave_field_input does not contain necessary values.
    """
    leave_input_value = 0
    try:
        leave_input_value = float(leave_field_input.value)
        if not leave_field_input.operation:
            logger_object.info("Leave_field_input mandatory fields are not given.")
            return None

        return leave_input_value

    except ValueError as e:
        logger_object.error("Error occurred while converting {}".format(e))
        logger_object.error("Not able to convert {} to Float".format(leave_field_input.value))
        return None


class BulkChange(graphene.Mutation):
    """Class for Bulk change Designation,Employee Type,Direct Manager,Shift,Leave balance of employee codes.

    Args:
        hrms_ids (list): List of hrms id's which we want to bulk change

        general_field_input(object): Object contains GeneralFieldInputObj
                                     attributes

        leave_field_input(object): Object contains LeaveFieldInputObj
                                   attributes
        ok(field): True if BulkChange operation perfrom successfully otherwise
                   false
    """

    class Arguments:
        """A class to define arguments for GraphQL mutation."""

        hrms_ids = graphene.List(graphene.String)
        general_field_input = graphene.Argument(GeneralFieldInputObj)
        leave_field_input = graphene.Argument(LeaveFieldInputObj)

    # Status code for success in inserting data in mongodb.
    ok = graphene.Field(BulkUnionObjects)

    @classmethod
    @audit
    @mutation_header_jwt_required
    @authorization
    def mutate(
        cls,
        _,
        info,
        leave_field_input=None,
        general_field_input=None,
        hrms_ids=None,
    ):
        """Resolve BulkChange mutation.

        Args:
            leave_field_input (dict, optional): It contains leave related bulk
                                                change data. Defaults to None.
            general_field_input (dict, optional): It contains general field
                                                  related bulk change data. Defaults to None.
            hrms_ids (list, optional): All hrms id's on which operation to be performed. Defaults to None.

        Returns:
            (object): It return BulkChange object
        """
        if not hrms_ids:
            logger_object.info("Hrms id's are not given")
            return BulkChange(ok=OkFieldObj(ok=False))

        # Input value for bulk leave balance change
        if leave_field_input:
            leave_input_value = get_leave_input_value(leave_field_input)
            if leave_input_value is None:
                return BulkChange(ok=OkFieldObj(ok=False))
        try:
            # Get Username from JWT token
            user_name = get_user_from_jwt(request)
            logger_object.debug("User name is {}".format(user_name))
            logger_object.debug("Hrms id's are {}".format(hrms_ids))
            logger_object.debug("General field input is {}".format(general_field_input))
            logger_object.debug("Leave field input is {}".format(leave_field_input))

            _, hrms_id = read_jwt(request)
            user_role = get_role_from_db(hrms_id)

            # Get all employee objects from
            # employeeDetailsModel collection
            # using hrms id's
            employees = EmployeeDetails.objects(_id__in=hrms_ids)

            # The API is accessible by PMO and Admin only.
            # so the condition will check whether the role
            # is Admin or PMO.
            # If general field input in present
            # condition will be True
            if general_field_input:
                # HRMS-2784 - get prev DM details of all employees
                # cannot go inside if loop as the DB is getting
                # updated before fetching prev dm details
                emp_details_list = []
                for hrmsid in hrms_ids:
                    if hrmsid:
                        emp_details = {}
                        employee = json.loads(EmployeeDetails.objects(_id=hrmsid).to_json())[0]
                        emp_details["hrms_id"] = hrmsid
                        emp_details["emp_code"] = employee["emp_code"]
                        emp_details["first_name"] = employee["first_name"]
                        emp_details["email"] = employee["email"]
                        emp_details["prev_dm"] = employee.get("direct_manager", "")
                        emp_details["old_designation"] = employee["designation"]
                        emp_details["old_grade"] = employee["grade"]
                        emp_details["saral"] = employee.get("saral", "")
                        emp_details["old_emp_type"] = employee["employee_type"]
                        emp_details_list.append(emp_details)
                # If role is Admin,
                # function will allow him/her
                # to update all fields.
                if "Admin" in user_role:
                    ok = update_all_fields_for_bulk_change(hrms_ids, general_field_input, emp_details_list, employees)
                    if not ok:
                        return BulkChange(ok=OkFieldObj(ok=False))

                # If role is not Admin it will be PMO,
                # so, PMO is allowed to update only direct manager and buissness group of the user
                if "PMO" in user_role and "Admin" not in user_role:
                    update_dm_and_buissness_group_of_the_user(
                        hrms_ids, general_field_input, emp_details_list, employees
                    )

            # Object of queries
            # ADD_TO_EXISTING:      Increment current balance by given value
            # REMOVE_FROM_EXISTING: Decrement current balance by given value
            # REPLACE_ALL_WITH :    Replace current balance by given value

            # Get all employee objects from
            # leave collection using hrms id's
            # In current implementation we have only
            # One leave type annual leave though
            # It can be extended with different types
            employees = Leave.objects.filter(
                hrms_id__in=hrms_ids,
                master_leave_balance__match={"leave_type": LeaveType.ANNUAL_LEAVE.value},
            )

            # If user wants to update Leave Balance
            # condition will be True
            if leave_field_input and ("Admin" in user_role or "PMO" in user_role):
                update_leave_balance_of_the_user(leave_field_input, leave_input_value, employees)

        except CompanySwitched:
            ok = False
            raise
        except Exception as e:
            logger_object.error("Exception occurred while bulk changing {}".format(e))
            return BulkChange(ok=OkFieldObj(ok=False))

        return BulkChange(ok=OkFieldObj(ok=True))