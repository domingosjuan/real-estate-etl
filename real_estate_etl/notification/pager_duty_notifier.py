from notification.abstractions.abstract_notifier import AbstractNotifier

class PagerDutyNotifier(AbstractNotifier):

    def notify(self):
        print("Sending Pager Duty Notification")