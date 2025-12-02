import os

def replace_in_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    new_content = content.replace('from apps.core', 'from core')
    
    if content != new_content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Updated {filepath}")

base_dir = os.path.join('apps', 'webapp')
for root, dirs, files in os.walk(base_dir):
    for file in files:
        if file.endswith('.py'):
            replace_in_file(os.path.join(root, file))
