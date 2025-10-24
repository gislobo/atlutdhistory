import requests
from bs4 import BeautifulSoup


def get_match_data(url):
    """
    Scrapes date and attendance information from Transfermarkt match schedule page.
    Returns a list of dictionaries with match information.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching page: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')

    # Debug: Save the HTML to see what we're getting
    with open('Archive/debug_page.html', 'w', encoding='utf-8') as f:
        f.write(soup.prettify())
    print("HTML saved to debug_page.html for inspection")

    matches = []

    # Try different selectors
    table = soup.find('table', {'class': 'items'})

    if not table:
        # Try alternative selectors
        table = soup.find('div', {'class': 'responsive-table'})
        if table:
            table = table.find('table')

    if not table:
        # Look for any table
        tables = soup.find_all('table')
        print(f"Found {len(tables)} tables on the page")
        if tables:
            table = tables[0]  # Try the first table
            print("Using first table found")

    if not table:
        print("Could not find match table")
        print("\nAvailable divs with 'box' class:")
        boxes = soup.find_all('div', class_=lambda x: x and 'box' in x)
        for box in boxes[:5]:
            print(f"  - {box.get('class')}")
        return []

    # Get all match rows
    rows = table.find_all('tr')
    print(f"Found {len(rows)} rows in table")

    for idx, row in enumerate(rows):
        cells = row.find_all(['td', 'th'])
        if len(cells) < 3:
            continue

        # Skip header rows
        if cells[0].name == 'th':
            continue

        match_info = {}

        # Debug: Print cell contents
        print(f"\nRow {idx}: {len(cells)} cells")
        for i, cell in enumerate(cells[:10]):  # First 10 cells
            print(f"  Cell {i}: {cell.get_text(strip=True)[:50]}")

        # Try to extract data based on position
        if len(cells) >= 5:
            match_info['date'] = cells[1].get_text(strip=True) if len(cells) > 1 else cells[0].get_text(strip=True)

            # Look for opponent
            for cell in cells:
                if cell.find('a', href=lambda x: x and '/verein/' in x):
                    match_info['opponent'] = cell.get_text(strip=True)
                    break

            # Look for attendance (numbers > 1000)
            for cell in cells:
                text = cell.get_text(strip=True)
                clean = text.replace('.', '').replace(',', '').replace(' ', '')
                if clean.isdigit() and len(clean) >= 4:
                    match_info['attendance'] = text
                    break

        if match_info and match_info.get('date'):
            matches.append(match_info)

    return matches


def get_date_attendance_tuples(url):
    """
    Returns a list of tuples: (date, attendance)
    """
    matches = get_match_data(url)
    return [(m.get('date'), m.get('attendance')) for m in matches]


def get_date_attendance_dict(url):
    """
    Returns a dictionary with dates as keys and attendance as values.
    """
    matches = get_match_data(url)
    return {m.get('date'): m.get('attendance') for m in matches if m.get('date')}


# Main execution
if __name__ == "__main__":
    url = "https://www.transfermarkt.us/atlanta-united-fc/spielplandatum/verein/51663/plus/1?saison_id=2020&wettbewerb_id=&day=&heim_gast=&punkte=&datum_von=&datum_bis="

    print("Fetching match data...\n")

    # Option 1: List of dictionaries (most flexible)
    matches = get_match_data(url)
    print("\n=== Full Match Data (Dictionaries) ===")
    for i, match in enumerate(matches, 1):
        print(f"{i}. {match}")

    print("\n" + "=" * 50 + "\n")

    # Option 2: List of tuples
    tuples = get_date_attendance_tuples(url)
    print("=== Date & Attendance (Tuples) ===")
    for date, attendance in tuples:
        print(f"Date: {date}, Attendance: {attendance}")

    print("\n" + "=" * 50 + "\n")

    # Option 3: Dictionary
    date_dict = get_date_attendance_dict(url)
    print("=== Date & Attendance (Dictionary) ===")
    for date, attendance in date_dict.items():
        print(f"{date}: {attendance}")