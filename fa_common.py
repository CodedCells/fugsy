import json
import html
import logging
from bs4 import BeautifulSoup

def extract_submission_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    script_tag = soup.find('script', {'id': 'js-submissionData', 'type': 'application/json'})
    
    if script_tag and script_tag.string:
        return json.loads(script_tag.string)
    
    logging.debug('No submission data')
    return {}


def extract_figure_info(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    figures = soup.find_all('figure')
    submission_data = extract_submission_data(html_content)
    
    extracted_data = []
    
    for figure in figures:
        # Get ID
        raw_id = figure.get('id', '')
        fig_id = raw_id.replace('sid-', '') if raw_id else None

        # Start with submission data if available
        sub_data = submission_data.get(fig_id, {})
        data = {
            'id': fig_id,
            'rating': None,
            'thumbnail_url': None,
            'tags': [],
            'title': html.unescape(sub_data.get('title', '')).strip() or None,
            'user': html.unescape(sub_data.get('lower', '')).strip() or None,
            'display_name': html.unescape(sub_data.get('username', '')).strip() or None,
            'description': html.unescape(sub_data.get('description', '')).strip() or None
        }

        # Only fill in missing data from HTML content
        if not data['rating']:
            rating_class = next((cls for cls in figure.get('class', []) if cls.startswith('r-')), None)
            if rating_class:
                data['rating'] = rating_class[2:]

        if not data['thumbnail_url'] or not data['tags']:
            img_tag = figure.find('img')
            if img_tag:
                if not data['thumbnail_url']:
                    data['thumbnail_url'] = img_tag.get('src')
                if not data['tags']:
                    tags = img_tag.get('data-tags', '')
                    data['tags'] = tags.split()

        if not data['title'] or not data['user'] or not data['display_name']:
            caption = figure.find('figcaption')
            if caption:
                if not data['title']:
                    title_anchor = caption.find('a', title=True)
                    if title_anchor:
                        data['title'] = html.unescape(title_anchor.text.strip())

                by_element = caption.find('i', string='by')
                if by_element:
                    user_tag = by_element.find_next_sibling('a', href=True)
                    if user_tag:
                        if not data['user']:
                            data['user'] = user_tag.get('href').strip('/').split('/')[-1]
                        if not data['display_name']:
                            data['display_name'] = user_tag.text.strip()

        extracted_data.append(data)
    
    return extracted_data