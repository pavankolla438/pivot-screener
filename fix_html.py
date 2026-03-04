import re

with open('templates/index.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Remove Exch th headers
content = re.sub(r'<th onclick="sortTable\(\'[^\']+\',\'Exchange\'\)">[^<]*</th>\n?', '', content)

# Remove plain Exch th (no onclick)
content = re.sub(r'<th onclick="sortTable\(\'[^\']+\',\'Exchange\'\)[^"]*">[^<]*</th>\n?', '', content)

# Remove exBadge td cells
content = content.replace('<td>${exBadge}</td>', '')

with open('templates/index.html', 'w', encoding='utf-8') as f:
    f.write(content)

print('Done')