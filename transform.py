import argparse
import yaml

from bmf.engine import TransformationEngine
 

def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments as a dict

    Returns
    -------
    argparse.Namespace
        The parsed arguments into an
        argparse namespace
    """
    parser = argparse.ArgumentParser(
        description="Transformation executer"
    )
    parser.add_argument(
        "-c",
        "--config",
        nargs=1,
        help="Configuration file to use for transformation",
    )

    # Parse commandline args
    args = parser.parse_args()
    return args


if __name__ == '__main__':

    args = parse_args()

    with open(args.config[0], "r") as f:
        config = yaml.safe_load(f)

    engine = TransformationEngine(config)
    engine.run()
