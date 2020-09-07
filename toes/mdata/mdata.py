from flask import Blueprint, jsonify, request

from common.input_validation import extract_personal_reason
from common.settings import settings
from ew_common.input_validation import (
    extract_city_state,
    extract_name,
    extract_phone_number,
    extract_postal_code,
)
from ew_common.mobile_commons import send_sms
from models.chat_profile import ChatProfile
from models.chat_referral import ChatReferral

mod = Blueprint("mdata", __name__)

TECH_SANDBOX_CAMPAIGN_ID = 189718

ERROR_RESPONSE = {"message": "Sorry, I've hiccupped! Try writing to me tomorrow."}


class Flow:
    state_to_processor = {}

    def __init__(self, initial_state="state_initial"):
        for processor_method_name in dir(self):
            if processor_method_name.startswith("_state_"):
                state = processor_method_name.replace("_state_", "state_")
                self.state_to_processor[state] = getattr(self, processor_method_name)
        self.current_state = initial_state
        assert self.current_state in self.state_to_processor

    def result_transition(self, next_state, incoming_message):
        return {"next_state": next_state, "incoming_message": incoming_message}

    def result_send_message(self, message):
        return {"send_message": message}

    def result_send_message_and_transition(self, message, next_state, incoming_message):
        return {
            "send_message": message,
            "next_state": next_state,
            "incoming_message": incoming_message,
        }

    def result_start_message_and_transition(
        self, message, next_state, incoming_message
    ):
        return {
            "start_message": message,
            "next_state": next_state,
            "incoming_message": incoming_message,
        }

    def process_message(self, incoming_message, profile):
        message = ""
        while True:
            result = self.state_to_processor[self.current_state](
                incoming_message, profile
            )

            if result.get("next_state"):
                print(
                    f"Transitioning from {self.current_state} to {result['next_state']}"
                )
                self.current_state = result["next_state"]

            if result.get("start_message"):
                message += result.get("start_message")

            if result.get("send_message"):
                message += result.get("send_message")
                return message

            incoming_message = result["incoming_message"]

        raise RuntimeError("process_message did not end in a respnse message")


class ReferFlow(Flow):
    def _state_initial(self, incoming_message, profile):
        self.reset_referee(profile)
        return self.result_start_message_and_transition(
            "Let's get more people to join in this fight!\n",
            "state_receive_personal_reason",
            incoming_message,
        )

    # TODO get referrer name if we don't have?
    # TODO get referrer zip if we don't have?

    def _state_receive_personal_reason(self, incoming_message, profile):
        if incoming_message:
            personal_reason = extract_personal_reason(incoming_message)
            profile.refer_personal_reason = personal_reason
            incoming_message = None

        if profile.refer_personal_reason:
            return self.result_transition("state_receive_name", incoming_message)

        profile.refer_personal_reason = None
        return self.result_send_message(
            "First, in one quick sentence, why are you supporting Elizabeth? We'll share this with people you refer."
        )

    def _state_receive_name(self, incoming_message, profile):
        if incoming_message:
            first_and_last_name, first_name, last_name = extract_name(incoming_message)
            if not first_name or not last_name:
                return self.result_send_message("Reply with your first and last name.")

            profile.first_and_last_name = first_and_last_name
            profile.first_name = first_name
            profile.last_name = last_name
            incoming_message = None

        if profile.first_name:
            return self.result_transition("state_receive_postal_code", incoming_message)

        profile.first_last_name = None
        profile.last_name = None
        profile.first_name = None
        return self.result_send_message("What's your first and last name?")

    def _state_receive_postal_code(self, incoming_message, profile):
        if incoming_message:
            postal_code = extract_postal_code(incoming_message)
            if not postal_code:
                return self.result_send_message("Reply with your 5-digit zip code.")

            profile.postal_code = postal_code
            incoming_message = None

        if profile.postal_code:
            return self.result_transition(
                "state_receive_referee_name", incoming_message
            )

        profile.postal_code = None
        return self.result_send_message("And what's your zip code?")

    def _state_receive_referee_name(self, incoming_message, profile):
        if incoming_message:
            referee_first_and_last_name, referee_first_name, referee_last_name = extract_name(
                incoming_message
            )
            if not referee_first_name or not referee_last_name:
                return self.result_send_message(
                    "Reply with your friend's first and last name."
                )

            profile.refer_referee_first_and_last_name = referee_first_and_last_name
            profile.refer_referee_first_name = referee_first_name
            profile.refer_referee_last_name = referee_last_name
            incoming_message = None

        if profile.refer_referee_first_name and profile.refer_referee_last_name:
            return self.result_transition(
                "state_receive_referee_phone_number", incoming_message
            )

        profile.refer_referee_first_and_last_name = None
        profile.refer_referee_first_name = None
        profile.refer_referee_last_name = None
        return self.result_send_message("What's your friend's first and last name?")

    def _state_receive_referee_phone_number(self, incoming_message, profile):
        if incoming_message:
            referee_phone_number = extract_phone_number(incoming_message)
            if referee_phone_number:
                profile.refer_referee_phone_number = referee_phone_number
                incoming_message = None

        if profile.refer_referee_phone_number:
            return self.result_transition(
                "state_receive_referee_city_state", incoming_message
            )

        return self.result_send_message(
            f"Cool, what's {profile.refer_referee_first_and_last_name}'s phone number?"
        )

    def _state_receive_referee_city_state(self, incoming_message, profile):
        if incoming_message:
            referee_city, referee_state = extract_city_state(incoming_message)
            if not referee_city or not referee_state:
                return self.result_send_message(
                    "Reply with your friend's city and state. For example: Fort Dodge IA"
                )
            profile.refer_referee_city = referee_city
            profile.refer_referee_state = referee_state
            incoming_message = None

        if profile.refer_referee_city and profile.refer_referee_state:
            return self.result_transition("state_send_referral", incoming_message)

        profile.refer_referee_city = None
        profile.refer_referee_state = None
        return self.result_send_message(
            f"What city and state does {profile.refer_referee_first_name} live in? For example: Reno NV"
        )

    def _state_send_referral(self, incoming_message, profile):
        referral_message = f"Your friend {profile.first_name} {profile.last_name} invited you to join the fight with Elizabeth Warren. {profile.refer_referee_first_name}, we need you in this fight too. Reply FIGHT to join."
        send_sms(
            settings.mobile_commons_username,
            settings.mobile_commons_password,
            TECH_SANDBOX_CAMPAIGN_ID,
            profile.refer_referee_phone_number,
            referral_message,
        )
        followup_referral_message = f'{profile.first_name} supports Elizabeth because: "{profile.refer_personal_reason}"'
        send_sms(
            settings.mobile_commons_username,
            settings.mobile_commons_password,
            TECH_SANDBOX_CAMPAIGN_ID,
            profile.refer_referee_phone_number,
            followup_referral_message,
        )

        result = self.result_send_message_and_transition(
            f"Great, sent an invite to {profile.refer_referee_first_name}. What's the name of another person you'd like to invite to join the fight?",
            "state_receive_referee_name",
            incoming_message,
        )

        referral = ChatReferral.get_or_create_referral(
            profile.refer_referee_phone_number, profile.phone_number
        )

        referral.first_and_last_name = profile.refer_referee_first_and_last_name
        referral.first_name = profile.refer_referee_first_name
        referral.last_name = profile.refer_referee_last_name
        referral.city = profile.refer_referee_city
        referral.state = profile.refer_referee_state

        referral.referrer_first_and_last_name = profile.first_and_last_name
        referral.referrer_first_name = profile.first_name
        referral.referrer_last_name = profile.last_name
        referral.referrer_postal_code = profile.postal_code
        if profile.email:
            referral.referrer_email = profile.email

        referral.save()

        self.reset_referee(profile)

        return result

    def reset_referee(self, profile):
        profile.refer_referee_first_and_last_name = None
        profile.refer_referee_first_name = None
        profile.refer_referee_last_name = None
        profile.refer_referee_phone_number = None
        profile.refer_referee_city = None
        profile.refer_referee_state = None
        profile.refer_referee_state = None


@mod.route("/refer")
def refer():
    incoming_message = request.args.get("args", "").strip()

    phone_number = request.args.get("phone", "").strip()
    if not phone_number:
        print(f"No incoming phone number. Args were: {request.args}")
        return jsonify(ERROR_RESPONSE), 422

    profile = ChatProfile.get_or_create_profile(phone_number)

    first_and_last_name = request.args.get("profile_first_and_last_name", "")
    if first_and_last_name:
        profile.first_and_last_name = first_and_last_name
    first_name = request.args.get("profile_first_name", "")
    if first_name:
        profile.first_name = first_name
    last_name = request.args.get("profile_last_name", "")
    if last_name:
        profile.last_name = last_name
    email = request.args.get("profile_email", "")
    if email:
        profile.email = email
    postal_code = request.args.get("profile_postal_code", "")
    if postal_code:
        profile.postal_code = postal_code

    if incoming_message:
        last_state = profile.last_state
    else:
        # If the user sent only the keyword, we start the flow from the beginning.
        last_state = None

    print(
        f"Setting up refer flow for {phone_number} with initial state {last_state} responding to {incoming_message}"
    )
    if last_state:
        flow = ReferFlow(last_state)
    else:
        flow = ReferFlow()
    message = flow.process_message(incoming_message, profile)
    profile.last_state = flow.current_state

    # TODO: Use profile.update()? Only save if actual modified?
    profile.set_updated_at()
    profile.save()

    return jsonify({"message": message})


@mod.route("/debt")
def debt():
    incoming_message = request.args.get("args", "").strip()
    first_name = request.args.get("profile_first_name", "")
    income_last_year = int(request.args.get("profile_techsandbox_income_last_year", ""))
    outstanding_student_loan_debt = int(
        request.args.get("profile_techsandbox_outstanding_student_loan_debt", "")
    )

    if income_last_year and int(incoming_message) == income_last_year:
        qualifies = income_last_year < 250000
        if qualifies:
            if income_last_year <= 100000:
                relief = 50000
            else:
                relief = round(50000 - ((income_last_year - 100000) / 3))
        else:
            relief = 0
        student_loan_cancellation = min(relief, outstanding_student_loan_debt)

        remaining_student_loans = round(
            max(outstanding_student_loan_debt - student_loan_cancellation, 0)
        )

        if qualifies:
            if remaining_student_loans == 0:
                message = "Congratulations, you'll be debt free!"
            else:
                message = f"Great news! You'll have ${student_loan_cancellation} of debt cancelled under Elizabeth's plan, bringing your outstanding student debt down to ${remaining_student_loans}."
            followupMessage = "\n\nIf you know someone else who might benefit from student debt cancellation, what's their phone number?"
            message = message + followupMessage
        else:
            message = "You don't qualify but it's still a great plan!"
    else:
        phone_number = extract_phone_number(incoming_message)
        if phone_number:
            message = "Great, sent! If you know others still with student loan debt, reply with their phone number."
            referral_message = f"Hi! Your friend {first_name} thought you'd be interested in Elizabeth Warren's plan to cancel student loan debt. Reply SANDBOXDEBT to see how much you'd save."

            send_sms(
                settings.mobile_commons_username,
                settings.mobile_commons_password,
                TECH_SANDBOX_CAMPAIGN_ID,
                phone_number,
                referral_message,
            )
        else:
            message = "To share the student debt calculator with a friend, reply with their phone number! (including area code)"

    return jsonify({"message": message})
