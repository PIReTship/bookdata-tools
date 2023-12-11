function load_overrides()
    local path, f, text
    if overrides == nil then
        path = pandoc.path.join({ quarto.project.directory, "_data-overrides.json" })
        f = io.open(path)
        if f == nil then
            quarto.log.warning("could not open data overrides")
            overrides = {}
            return
        end
        text = f:read("a")
        overrides = quarto.json.decode(text)
        quarto.log.debug(overrides)
    end
end

function load_schema(file)
    local path, base, ext, f, text
    path = pandoc.path.join({ quarto.project.directory, "..", file })
    base, ext = pandoc.path.split_extension(path)
    path = base .. ".json"
    quarto.log.debug("reading schema file", path)

    f = io.open(path)
    if f == nil then
        quarto.log.warning("could not open schema file", path)
        return nil
    end

    text = f:read("a")
    return quarto.json.decode(text)
end

return {
    ['file'] = function(args, kwargs, meta)
        local path, fn, name, link, pat, sub, m, ms
        load_overrides()
        path = pandoc.utils.stringify(args[1])
        quarto.log.debug('finding link for', path)

        for pat, sub in pairs(overrides) do
            quarto.log.trace('trying pattern', pat)
            m = string.match(path, pat)
            if m ~= nil and (ms == nil or string.len(m) > string.len(ms)) then
                quarto.log.trace('match')
                ms = m
                if string.find(sub, "%", 1, true) ~= nil then
                    quarto.log.trace('substition', sub)
                    link = string.gsub(path, pat, sub)
                else
                    fn = sub
                    name = string.gsub(path, "[^/]+/", "")
                end
            end
        end

        if link == nil then
            if fn == nil then
                fn, name = string.match(path, "^([^/].*)/(.*)")
            end
            if fn == nil then
                link = "#" .. path
            else
                link = fn .. "#" .. name
            end
        end
        link = string.gsub(link, "(.+)#(.*)", "/data/%1.qmd#file:%2")

        return pandoc.Link(pandoc.Code(path), link)
    end,

    ['schema'] = function(args, kwargs, meta)
        local path, schema, rows
        path = pandoc.utils.stringify(args[1])
        schema = load_schema(path)
        if schema == nil then
            return pandoc.Div({
                pandoc.Str("Could not find schema file "),
                pandoc.Code(path),
                pandoc.Str(".")
            })
        end

        header = pandoc.Row({
            pandoc.Cell({ pandoc.Div("Field") }),
            pandoc.Cell({ pandoc.Div("Type") }),
        })
        rows = pandoc.List()
        for i, field in ipairs(schema.fields) do
            rows[#rows + 1] = pandoc.Row({
                pandoc.Cell({ pandoc.Div(field.name) }),
                pandoc.Cell({ pandoc.Div(field.data_type) }),
            })
        end

        local caption = {"Schema for ", pandoc.Code(path), "."}
        return pandoc.Table({ long = { pandoc.Div({caption}) }, short = caption },
            { { pandoc.AlignLeft, 60 }, { pandoc.AlignRight, 40 } },
            pandoc.TableHead({ header }),
            { { attr = pandoc.Attr(), body = rows, head = {}, row_head_columns = 0 } }, pandoc.TableFoot(), pandoc.Attr())
    end,
}
