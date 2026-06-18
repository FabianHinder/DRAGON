import pandas as pd


class HTMLConverter:
    def __init__(self):
        self.sections = []

    def add_explainer_results(self, title, explainer):
        if not explainer.results:
            self.sections.append(f"<h3>{title}</h3><p>No results.</p>")
            return
        data = []
        for r in explainer.results:
            row = {'index': r['index'], 'reason': r.get('reason', 'N/A')}
            if isinstance(r['result'], dict):
                row.update(r['result'])
            else:
                row['value'] = r['result']
            data.append(row)

        df = pd.DataFrame(data)
        self.sections.append(f"<h3>{title}</h3>" + df.to_html(index=False, classes='table'))

    def add_plots_from_explainer(self, explainer):
        self.sections.append("<h3>Visual Analysis</h3>")
        found_plots = 0

        for r in explainer.results:
            res = r.get('result')
            if isinstance(res, dict) and 'plot_html' in res:
                img_html = f'''
                <div style="margin: 20px 0; border: 1px solid #eee; padding: 10px;">
                    <p><b>Event at Index {r['index']}</b></p>
                    <img src="{res['plot_html']}" style="max-width: 100%; height: auto;" />
                </div>
                '''
                self.sections.append(img_html)
                found_plots += 1

        if found_plots == 0:
            self.sections.append("<p>No plots found in this explainer.</p>")

    def generate(self, filename="report.html"):
        style = "<style>body{font-family:sans-serif;margin:40px}.table{width:100%;border-collapse:collapse} td,th{border:1px solid #ddd;padding:8px} th{background:#f2f2f2}</style>"
        content = "".join(self.sections)
        with open(filename, "w") as f:
            f.write(f"<html><head>{style}</head><body>{content}</body></html>")
        print(f"Report generated: {filename}")
