#!/usr/bin/env python
# encoding: utf-8
"""
Mailer.py
Created by Robert Dempsey on 11/07/14.
Copyright (c) 2014 Robert Dempsey. All rights reserved.
"""

import sys
import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

COMMASPACE = ', '

class Mailer:
    def __init__(self, **kwargs):
        self.properties = kwargs

    # Subject
    @property
    def subject(self):
        return self.properties.get('subject', 'None')

    @subject.setter
    def subject(self, s):
        self.properties['subject'] = s

    @subject.deleter
    def subject(self):
        del self.properties['subject']

    # Recipients
    @property
    def recipients(self):
        return self.properties.get('recipients', 'None')

    @recipients.setter
    def recipients(self, r):
        self.properties['recipients'] = r

    @recipients.deleter
    def recipients(self):
        del self.properties['recipients']

    # Send From
    @property
    def send_from(self):
        return self.properties.get('send_from', 'None')

    @send_from.setter
    def send_from(self, s_from):
        self.properties['send_from'] = s_from

    @send_from.deleter
    def send_from(self):
        del self.properties['send_from']

    # Gmail Password
    @property
    def gmail_password(self):
        return self.properties.get('gmail_password', 'None')

    @gmail_password.setter
    def gmail_password(self, g_pass):
        self.properties['gmail_password'] = g_pass

    @gmail_password.deleter
    def gmail_password(self):
        del self.properties['gmail_password']

    # Message
    @property
    def message(self):
        return self.properties.get('message', 'None')

    @message.setter
    def message(self, m):
        self.properties['message'] = m

    @message.deleter
    def message(self):
        del self.properties['message']

    # Attachments
    @property
    def attachments(self):
        return self.properties.get('attachments', 'None')

    @attachments.setter
    def attachments(self, a):
        self.properties['attachments'] = a

    @attachments.deleter
    def attachments(self):
        del self.properties['attachments']

    def send_email(self):
        # Create the enclosing (outer) message
        outer = MIMEMultipart('alternative')
        outer['Subject'] = self.subject
        outer['To'] = COMMASPACE.join(self.recipients)
        outer['From'] = self.send_from

        msg = MIMEBase('application', "octet-stream")

        # Add the text of the email
        email_body = MIMEText(self.message, 'plain')
        outer.attach(email_body)

        # Add the attachments (If the attachments are exist)
        if str(self.attachments) != 'None':
            for file in self.attachments:
                try:
                    with open(file, 'rb') as fp:
                        msg = MIMEBase('application', "octet-stream")
                        msg.set_payload(fp.read())
                    encoders.encode_base64(msg)
                    msg.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file))
                    outer.attach(msg)
                except:
                    print("Unable to open one of the attachments. Error: ", sys.exc_info()[0])
                    raise


        composed = outer.as_string()

        try:
            with smtplib.SMTP('smtp.gmail.com', 587) as s:
                s.ehlo()
                s.starttls()
                s.ehlo()
                s.login(self.send_from, self.gmail_password)
                s.sendmail(self.send_from, self.recipients, composed)
                s.close()
            print("Email sent!")
        except:
            print("Unable to send the email. Error: ", sys.exc_info()[0])
            raise

def main():
    pass

if __name__ == '__main__': main()