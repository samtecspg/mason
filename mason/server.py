import connexion
import markdown

from mason.definitions import from_root
from mason.util.printer import banner
from mason.util.environment import MasonEnvironment
from mason.util.logger import logger
from mason.operators import operators

class MasonServer:
    def run(self):
        try:
            banner("Importing all registered_operator modules for API")
            env = MasonEnvironment()
            operators.import_all(env)

            base_swagger = from_root("/api/base_swagger.yml")

            banner(f"Regenerating api yaml based on registered_operators to {base_swagger}")

            operators.update_yaml(env, base_swagger)

            app = connexion.App(__name__, specification_dir='api')

            # Read the swagger.yml file to configure the endpoints
            swagger = from_root("/api/swagger.yml")
            app.add_api(swagger)

            # Create a URL route in our application for "/"
            @app.route('/')
            def home():
                """
                This function just responds to the browser ULR
                localhost:5000/
                :return:        the rendered template 'home.html'
                """

                readme_file = open("../README.md", "r")
                md_template_string = markdown.markdown(
                    readme_file.read(), extensions=["fenced_code"]
                )

                return md_template_string

            # If we're running in stand alone mode, run the application
            if __name__ == 'mason.server':
                app.run(host='0.0.0.0', port=5000, debug=True)

        except ModuleNotFoundError as e:
            logger.error("Mason not configured with registered_operators.  Please run 'mason config' and 'mason register' first.")
            logger.error(str(e))
