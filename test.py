import re


def read_file(filename):
    """
    Read the content of a file and return it as a list of lines.

    Args:
        filename (str): The name of the file to be read.

    Returns:
        list: A list containing the lines of the file.

    Example:
        content = read_file("sample.txt")
        print(content)
        # Output: ['line 1\n', 'line 2\n', 'line 3\n']
    """
    with open(filename, "r") as file:
        content = file.readlines()
    return content


def is_valid_email_character(character):
    return character.isalpha() or character.isdigit() or character in [".", "-", "_"]


def find_emails(line):
    return re.findall(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", line)


def find_phone_numbers(line):
    return re.findall(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b", line)


def extract_information_from_file(filename):
    lines = read_file(filename)
    all_emails = [email for line in lines for email in find_emails(line)]
    all_phone_numbers = [
        phone_number for line in lines for phone_number in find_phone_numbers(line)
    ]
    return all_emails, all_phone_numbers


def write_to_file(filename: str, emails: list, phone_numbers: list) -> None:
    """
    Writes the extracted emails and phone numbers to a file.

    Args:
        filename (str): The name of the output file.
        emails (list): A list of email addresses.
        phone_numbers (list): A list of phone numbers.

    Returns:
        None. The function writes the extracted emails and phone numbers to the output file.
    """
    with open(filename, "w") as file:
        file.write("Emails:\n")
        file.write("\n".join(emails) + "\n")
        file.write("\nPhone Numbers:\n")
        file.write("\n".join(phone_numbers) + "\n")


def main():
    """
    The `main` function is the entry point of the program. It reads the content of a file, extracts emails and phone numbers from the file, and writes the extracted information to another file.

    Example Usage:
    ```python
    input_filename = "sample.txt"
    output_filename = "output.txt"
    emails, phone_numbers = extract_information_from_file(input_filename)
    write_to_file(output_filename, emails, phone_numbers)
    ```

    Inputs:
    - input_filename (str): The name of the input file.
    - output_filename (str): The name of the output file.

    Outputs:
    None. The function writes the extracted emails and phone numbers to the output file.
    """
    input_filename = "sample.txt"
    output_filename = "output.txt"
    emails, phone_numbers = extract_information_from_file(input_filename)
    write_to_file(output_filename, emails, phone_numbers)


if __name__ == "__main__":
    main()
