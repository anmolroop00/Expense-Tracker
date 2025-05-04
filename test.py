import imaplib
import email
from email.header import decode_header
import os
from datetime import datetime, timedelta
import calendar

# ========== USER CONFIGURATION ==========
username = ""
password = ""  # Use an App Password if 2FA is enabled
imap_server = "imap.gmail.com"
# =========================================

# Sanitize filenames and folder names
def clean(text):
    return "".join(c if c.isalnum() else "_" for c in text)

# Calculate date range for the previous month
today = datetime.today()
first_day_last_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
last_day_last_month = today.replace(day=1) - timedelta(days=1)

# Format to IMAP date format: "01-Apr-2025"
since_date = first_day_last_month.strftime("%d-%b-%Y")
before_date = (last_day_last_month + timedelta(days=1)).strftime("%d-%b-%Y")

# Month folder for saving attachments
month_folder = last_day_last_month.strftime("%b_%Y")
if not os.path.isdir(month_folder):
    os.makedirs(month_folder)

# Connect to Gmail IMAP
imap = imaplib.IMAP4_SSL(imap_server)
imap.login(username, password)

# Select inbox
imap.select("INBOX")

# Search emails from last month
status, messages = imap.search(None, f'(SINCE "{since_date}" BEFORE "{before_date}")')
email_ids = messages[0].split()

print(f"Found {len(email_ids)} emails from {since_date} to {before_date}")

# Loop through emails
for i in email_ids:
    res, msg_data = imap.fetch(i, "(RFC822)")
    for response in msg_data:
        if isinstance(response, tuple):
            msg = email.message_from_bytes(response[1])

            # Decode sender
            From, enc = decode_header(msg.get("From"))[0]
            if isinstance(From, bytes):
                From = From.decode(enc or "utf-8")

            # Decode subject (optional)
            subject, enc = decode_header(msg.get("Subject"))[0]
            if isinstance(subject, bytes):
                subject = subject.decode(enc or "utf-8")

            # Filter sender by keywords
            if "icici" in From.lower() or "onecard" in From.lower():
                print(f"From: {From}")
                print(f"Subject: {subject}")

                # Handle multipart messages
                if msg.is_multipart():
                    for part in msg.walk():
                        content_disposition = str(part.get("Content-Disposition"))
                        if "attachment" in content_disposition:
                            filename = part.get_filename()
                            if filename:
                                # Decode and clean filename
                                decoded_filename, encoding = decode_header(filename)[0]
                                if isinstance(decoded_filename, bytes):
                                    decoded_filename = decoded_filename.decode(encoding or "utf-8")

                                safe_filename = clean(decoded_filename)
                                filepath = os.path.join(month_folder, safe_filename)

                                # Save the attachment
                                with open(filepath, "wb") as f:
                                    f.write(part.get_payload(decode=True))

                                print(f"Saved attachment: {filepath}")
                print("=" * 60)

# Close the connection
imap.close()
imap.logout()
