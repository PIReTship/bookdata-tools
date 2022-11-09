from pathlib import Path

data_dir = Path('data')
tgt_dir = Path(__file__).parent.parent / 'target'
bin_dir = tgt_dir / 'release'
bdtool = bin_dir / 'bookdata'
