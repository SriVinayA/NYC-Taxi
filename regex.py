import re

def regex_demo():
    text = """
    Hello World!
    My email is test@example.com.
    You can also contact me at hello123@test.org or support@test.co.uk.
    My phone number is +1-202-555-0173.
    Today is 2025-09-26.
    """

    # 1. Match all email addresses
    emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    print("📧 Emails found:", emails)

    # 2. Match all dates in YYYY-MM-DD format
    dates = re.findall(r"\d{4}-\d{2}-\d{2}", text)
    print("📅 Dates found:", dates)

    # 3. Match all phone numbers like +1-202-555-0173
    phones = re.findall(r"\+\d{1,2}-\d{3}-\d{3}-\d{4}", text)
    print("📞 Phone numbers found:", phones)

    # 4. Find all words starting with a capital letter
    capital_words = re.findall(r"\b[A-Z][a-zA-Z]+\b", text)
    print("🔠 Capitalized words:", capital_words)

    # 5. Match domain names from emails
    domains = re.findall(r"@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", text)
    print("🌐 Domains:", domains)

    # 6. Replace all email addresses with [EMAIL HIDDEN]
    hidden_text = re.sub(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", "[EMAIL HIDDEN]", text)
    print("\n🔒 Text with hidden emails:\n", hidden_text)

if __name__ == "__main__":
    regex_demo()
