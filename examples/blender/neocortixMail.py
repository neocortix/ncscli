#!/usr/bin/env python3
"""
releses a hanging segment by deleting its _fetching flag file
"""

# standard library modules
#import argparse
#import getpass
import email
import email.mime.application
import email.mime.multipart
import email.mime.text
import logging
import os
import smtplib
import sys

logger = logging.getLogger(__name__)

g_defaultSmtpHost ='localHost'
g_passwordFilePath = 'emailPassword'

def readPasswordFromFile( inFilePath ):
    try:
        with open( os.path.expanduser(inFilePath), "r" ) as inFile:
            pw = inFile.readline().strip()
            return pw
    except Exception:
        print( 'could not read from %s' % (inFilePath), file=sys.stderr )
    return ''

def sendMailWithAttachments(sender, recipient, subject, body, smtpHost, attachmentPaths, pwd=None):
    'send mail using TLS (but not gmail-style SSL)'
    if pwd == None:
        pwd = readPasswordFromFile( g_passwordFilePath )
    TO = recipient if type(recipient) is list else [recipient]
    SUBJECT = subject

    # Prepare a multipart message
    msg = email.mime.multipart.MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ", ".join(TO)
    msg['Subject'] = SUBJECT
    msg.attach(email.mime.text.MIMEText(body, 'plain'))

    if attachmentPaths:
        for attachmentPath in attachmentPaths:
            if not attachmentPath:
                logger.warning( 'null or empty attachmentPath %s', attachmentPath )
                continue
            with open( attachmentPath, 'rb' ) as attFile:
                attContent = attFile.read()
            att = email.mime.application.MIMEApplication(attContent, 'octet-stream')
            #att = email.mime.text.MIMEText(attContent, 'plain')
            att.add_header('Content-Disposition', 'attachment', filename=os.path.basename( attachmentPath ))
            msg.attach( att )
    else:
        logger.warning( 'NO attachment given' )

    if not smtpHost:
        smtpHost = g_defaultSmtpHost
    try:
        server = smtplib.SMTP(smtpHost, 587)
        server.ehlo()
        server.starttls()
        server.login(sender, pwd)
        server.sendmail(sender, TO, msg.as_string() )
        server.close()
        logger.debug( 'sent mail' )
    except Exception as exc:
        logger.error( "failed to send mail" )
        print( exc, file=sys.stderr )
