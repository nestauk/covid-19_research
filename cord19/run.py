from cord19.data import make_rxivs
from cord19.data import make_rxivs_ai
from cord19.estimators import measure_research_deltas

def main():
    make_rxivs.main()
    make_rxivs_ai.main()
    measure_research_deltas.main()

if __name__ == '__main__':
    main()
