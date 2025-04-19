import os

def ask_yes_no(question):
    return input(question + " (y/n): ").strip().lower() == 'y'

def get_filename_input(prompt_text):
    while True:
        file_name = input(prompt_text).strip()
        if os.path.isfile(file_name):
            return file_name
        print("❌ File not found. Try again.")

def get_ip_input(prompt_text):
    return input(prompt_text).strip()

def get_subnet_mapping(prompt_text):
    old_subnet = input(f"{prompt_text} - Old subnet (e.g., 192.168.1.): ").strip()
    new_subnet = input(f"{prompt_text} - New subnet (e.g., 10.0.0.): ").strip()
    return old_subnet, new_subnet

def get_time_range(prompt_text):
    start = int(input(f"{prompt_text} - Start UNIX time: ").strip())
    end = int(input(f"{prompt_text} - End UNIX time: ").strip())
    return start, end

def get_time_shift(prompt_text):
    return int(input(f"{prompt_text} - Shift in seconds (e.g., 3600 or -1800): ").strip())

def process_file(filename, options):
    processed = []
    with open(filename, 'r') as f:
        for line in f:
            # === PLACEHOLDER: Parse fields from your line ===
            try:
                timestamp, proto, src_ip, rest = parse_line(line)
            except Exception:
                continue  # Skip bad lines

            # === Apply filters ===
            if options['extract_ip'] and not src_ip.startswith(options['ip_filter']):
                continue
            if options['modify_ip']:
                old_net, new_net = options['subnet_map']
                if src_ip.startswith(old_net):
                    src_ip = src_ip.replace(old_net, new_net, 1)
            if options['time_range']:
                start, end = options['time_range']
                if not (start <= timestamp <= end):
                    continue
            if options['shift_time']:
                timestamp += options['shift_seconds']

            # === REBUILD line ===
            new_line = build_line(timestamp, proto, src_ip, rest)
            processed.append((timestamp, new_line))

    return processed

def parse_line(line):
    """
    Placeholder for parsing a line into (timestamp, source_ip, rest_of_data)
    Example assumes tab-separated fields: [timestamp] [src_ip] [rest...]
    """
    parts = line.strip().split(' ')
    timestamp = float(parts[0])
    src_ip = parts[2]
    rest = ' '.join(parts[3:])
    return timestamp, parts[1], src_ip, rest

def build_line(timestamp, proto, src_ip, rest):
    """ Reconstruct line from components """
    return f"{timestamp} {proto} {src_ip} {rest}"

def main():
    print("== Log Processor & Merger (UNIX timestamp based) ==")

    file1 = get_filename_input("Enter path to first file: ")
    file2 = get_filename_input("Enter path to second file: ")

    options1 = {
        'extract_ip': ask_yes_no("Filter source IP in file 1?"),
        'ip_filter': '',
        'modify_ip': ask_yes_no("Modify IP/subnet in file 1?"),
        'subnet_map': ('', ''),
        'time_range': (),
        'shift_time': False,
        'shift_seconds': 0
    }

    if options1['extract_ip']:
        options1['ip_filter'] = get_ip_input("Source IP to filter (file 1):")
    if options1['modify_ip']:
        options1['subnet_map'] = get_subnet_mapping("File 1 subnet mapping")
    if ask_yes_no("Filter by time range in file 1?"):
        options1['time_range'] = get_time_range("File 1 time range")
    if ask_yes_no("Shift timestamps in file 1?"):
        options1['shift_time'] = True
        options1['shift_seconds'] = get_time_shift("File 1 time shift")

    options2 = {
        'extract_ip': ask_yes_no("Filter source IP in file 2?"),
        'ip_filter': '',
        'modify_ip': ask_yes_no("Modify IP/subnet in file 2?"),
        'subnet_map': ('', ''),
        'time_range': (),
        'shift_time': False,
        'shift_seconds': 0
    }

    if options2['extract_ip']:
        options2['ip_filter'] = get_ip_input("Source IP to filter (file 2):")
    if options2['modify_ip']:
        options2['subnet_map'] = get_subnet_mapping("File 2 subnet mapping")
    if ask_yes_no("Filter by time range in file 2?"):
        options2['time_range'] = get_time_range("File 2 time range")
    if ask_yes_no("Shift timestamps in file 2?"):
        options2['shift_time'] = True
        options2['shift_seconds'] = get_time_shift("File 2 time shift")

    # === Process both files ===
    processed1 = process_file(file1, options1)
    processed2 = process_file(file2, options2)

    # === Merge and sort by timestamp ===
    merged = sorted(processed1 + processed2, key=lambda x: x[0])

    # === Write to file ===
    out_file = input("Enter output merged filename: ").strip()
    with open(out_file, 'w') as f_out:
        for _, line in merged:
            f_out.write(line + '\n')

    print(f"✅ Merged output saved to {out_file}")

if __name__ == "__main__":
    main()
