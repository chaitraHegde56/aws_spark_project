from src.jobs.volume_transformations import calculate_volume


def test_calculate_volume():

    result = calculate_volume(10, 5, 2)

    assert result == 100