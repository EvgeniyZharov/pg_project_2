    def create_script_file(self, file_name: str, content: str):
        with open(file_name, 'w', encoding="utf-8") as script_file:
            script_file.write(content)

    def create_bat_file(self, file_name: str, script_title: str, timing: int):
        bat_program = f"""
        @echo off
        :loop
        python {script_title}
        timeout /t {timing} > nul
        goto loop
                                """
        self.SETTINGS["run_files"].append({"title": file_name, "timing": f"{timing}", "script": script_title})
        self.set_settings(self.settings_file)
        with open(file_name, 'w', encoding="utf-8") as bat_file:
            bat_file.write(bat_program)
