def test_installer_main_imports():
    import installer.__main__ as m

    assert callable(m.bootstrap)
    assert callable(m.start_servers)
    assert callable(getattr(m, "main", None))
