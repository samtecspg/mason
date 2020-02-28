
import argparse
import click

# @click.command()
# @click.argument('command')
@click.group()
def main():
    """
    Mason Data Operator Framework
    """
    pass
@main.command()
@click.argument('mason_file')
def deploy(mason_file: str):
    print(mason_file)

@main.command()
@click.argument('mason_file')
def validate(mason_file: str):
    print(mason_file)

if __name__ == "__main__":
    main()