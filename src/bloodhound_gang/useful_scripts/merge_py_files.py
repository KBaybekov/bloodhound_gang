#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import argparse

def main():
    extensions = ['.py', '.sh', '.txt', '.toml', '.yaml', '.yml', '.config', '.md', '.test', 'Dockerfile']
    parser = argparse.ArgumentParser(
        description='Объединяет все .py-файлы из заданной папки (рекурсивно) в один файл '
                    'с указанием относительного пути для каждого файла.'
    )
    parser.add_argument('folder', help='Путь к папке для поиска .py-файлов')
    parser.add_argument('-o', '--output', default='combined_output.txt',
                        help='Имя выходного файла (по умолчанию combined_output.txt)')
    args = parser.parse_args()

    root_dir = os.path.abspath(args.folder)
    if not os.path.isdir(root_dir):
        print(f"Ошибка: папка '{root_dir}' не существует.", file=sys.stderr)
        sys.exit(1)

    output_file = args.output

    with open(output_file, 'w', encoding='utf-8') as out_f:
        for dirpath, dirnames, filenames in os.walk(root_dir):
            for filename in filenames:
                for ext in extensions:
                    if filename.endswith(ext):
                        full_path = os.path.join(dirpath, filename)
                        rel_path = os.path.relpath(full_path, root_dir)

                        # Записываем заголовок с относительным путём
                        out_f.write(f"# File: {rel_path}\n")

                        # Читаем и записываем содержимое файла
                        try:
                            with open(full_path, 'r', encoding='utf-8') as in_f:
                                content = in_f.read()
                                out_f.write(content)
                                # Если содержимое не заканчивается переводом строки – добавляем его
                                if not content.endswith('\n'):
                                    out_f.write('\n')
                                # Пустая строка для разделения файлов
                                out_f.write('\n')
                        except Exception as e:
                            print(f"Предупреждение: не удалось прочитать файл {full_path}: {e}", file=sys.stderr)
                            out_f.write(f"# Ошибка чтения файла: {e}\n\n")

    print(f"Готово. Результат записан в {output_file}")

if __name__ == '__main__':
    main()